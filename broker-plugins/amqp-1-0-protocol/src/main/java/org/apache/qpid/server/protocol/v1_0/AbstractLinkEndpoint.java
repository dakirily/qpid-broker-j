/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.qpid.server.protocol.v1_0;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SenderSettleMode;

public abstract class AbstractLinkEndpoint<S extends BaseSource, T extends BaseTarget> implements LinkEndpoint<S, T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLinkEndpoint.class);
    private static final int FRAME_HEADER_SIZE = 8;

    private final Link_1_0<S, T> _link;
    private final Session_1_0 _session;

    private volatile SenderSettleMode _sendingSettlementMode;
    private volatile ReceiverSettleMode _receivingSettlementMode;
    private volatile UnsignedInteger _lastSentCreditLimit;
    private volatile boolean _stopped;
    private volatile boolean _stoppedUpdated;
    private volatile Symbol[] _capabilities;
    private volatile SequenceNumber _deliveryCount;
    private volatile UnsignedInteger _linkCredit;
    private volatile UnsignedInteger _available;
    private volatile Boolean _drain;
    private volatile UnsignedInteger _localHandle;
    private volatile Map<Symbol, Object> _properties;
    private volatile State _state = State.ATTACH_RECVD;
    private volatile boolean _errored = false;

    protected boolean _remoteIncompleteUnsettled;
    protected boolean _localIncompleteUnsettled;

    protected enum State
    {
        DETACHED,
        ATTACH_SENT,
        ATTACH_RECVD,
        ATTACHED,
        DETACH_SENT,
        DETACH_RECVD
    }

    AbstractLinkEndpoint(final Session_1_0 session, final Link_1_0<S, T> link)
    {
        _session = session;
        _link = link;
    }

    protected abstract void handleDeliveryState(final Binary deliveryTag, final DeliveryState state, final Boolean settled);

    protected abstract void remoteDetachedPerformDetach(final Detach detach);

    protected abstract Map<Symbol,Object> initProperties(final Attach attach);

    protected abstract Map<Binary, DeliveryState> getLocalUnsettled();

    @Override
    public void receiveAttach(final Attach attach) throws AmqpErrorException
    {
        _errored = false;
        boolean isAttachingRemoteTerminusNull = (attach.getRole() == Role.SENDER ? attach.getSource() == null : attach.getTarget() == null);
        boolean isAttachingLocalTerminusNull = (attach.getRole() == Role.SENDER ? attach.getTarget() == null : attach.getSource() == null);
        boolean isLocalTerminusNull = (attach.getRole() == Role.SENDER ? getTarget() == null : getSource() == null);

        if (isAttachingRemoteTerminusNull)
        {
            throw new AmqpErrorException(AmqpError.INVALID_FIELD, "received Attach with remote null terminus.");
        }

        if (isAttachingLocalTerminusNull)
        {
            recoverLink(attach);
        }
        else if (isLocalTerminusNull)
        {
            establishLink(attach);
        }
        else if (attach.getUnsettled() != null)
        {
            // TODO: QPID-7845 : Functionality for resuming links is not fully implemented
            if (attach.getUnsettled().isEmpty())
            {
                resumeLink(attach);
            }
            else
            {
                throw new AmqpErrorException(new Error(AmqpError.NOT_IMPLEMENTED, "Resuming link is not implemented."));
            }
        }
        else
        {
            reattachLink(attach);
        }
    }

    protected abstract void reattachLink(final Attach attach) throws AmqpErrorException;

    protected abstract void resumeLink(final Attach attach) throws AmqpErrorException;

    protected abstract void establishLink(final Attach attach) throws AmqpErrorException;

    protected abstract void recoverLink(final Attach attach) throws AmqpErrorException;

    public void attachReceived(final Attach attach) throws AmqpErrorException
    {
        _sendingSettlementMode = attach.getSndSettleMode();
        _receivingSettlementMode = attach.getRcvSettleMode();
        _properties = initProperties(attach);
        _state = State.ATTACH_RECVD;
        _remoteIncompleteUnsettled = Boolean.TRUE.equals(attach.getIncompleteUnsettled());
        if (getRole() == Role.RECEIVER)
        {
            getSession().getIncomingDeliveryRegistry().removeDeliveriesForLinkEndpoint(this);
        }
        else
        {
            getSession().getOutgoingDeliveryRegistry().removeDeliveriesForLinkEndpoint(this);
        }
    }

    public boolean isStopped()
    {
        return _stopped;
    }

    @Override
    public void setStopped(final boolean stopped)
    {
        if (_stopped != stopped)
        {
            _stopped = stopped;
            _stoppedUpdated = true;
            sendFlowConditional();
        }
    }

    public String getLinkName()
    {
        return _link.getName();
    }

    @Override
    public S getSource()
    {
        return _link.getSource();
    }

    @Override
    public T getTarget()
    {
        return _link.getTarget();
    }

    public NamedAddressSpace getAddressSpace()
    {
        return getSession().getConnection().getAddressSpace();
    }

    protected void setDeliveryCount(final SequenceNumber deliveryCount)
    {
        _deliveryCount = deliveryCount;
    }

    public void setLinkCredit(final UnsignedInteger linkCredit)
    {
        _linkCredit = linkCredit;
    }

    public void setAvailable(final UnsignedInteger available)
    {
        _available = available;
    }

    public void setDrain(final Boolean drain)
    {
        _drain = drain;
    }

    protected SequenceNumber getDeliveryCount()
    {
        return _deliveryCount;
    }

    public UnsignedInteger getAvailable()
    {
        return _available;
    }

    public Boolean getDrain()
    {
        return _drain;
    }

    public UnsignedInteger getLinkCredit()
    {
        return _linkCredit;
    }

    @Override
    public void remoteDetached(final Detach detach)
    {
        switch (_state)
        {
            case DETACH_SENT:
                _state = State.DETACHED;
                break;
            case ATTACHED:
                _state = State.DETACH_RECVD;
                remoteDetachedPerformDetach(detach);
                break;
        }
    }

    @Override
    public void receiveDeliveryState(final Binary deliveryTag,
                                     final DeliveryState state,
                                     final Boolean settled)
    {
        handleDeliveryState(deliveryTag, state, settled);

        if (Boolean.TRUE.equals(settled))
        {
            settle(deliveryTag);
        }
    }

    public void settle(final Binary deliveryTag)
    {

    }

    @Override
    public void setLocalHandle(final UnsignedInteger localHandle)
    {
        _localHandle = localHandle;
    }

    boolean isAttached()
    {
        return _state == State.ATTACHED;
    }

    boolean isDetached()
    {
        return _state == State.DETACHED || _session.isEnded();
    }

    @Override
    public Session_1_0 getSession()
    {
        return _session;
    }

    @Override
    public void destroy()
    {
        setLocalHandle(null);
        getLink().discardEndpoint();
    }

    @Override
    public UnsignedInteger getLocalHandle()
    {
        return _localHandle;
    }

    @Override
    public void sendAttach()
    {
        Attach attachToSend = new Attach();
        attachToSend.setName(getLinkName());
        attachToSend.setRole(getRole());
        attachToSend.setHandle(getLocalHandle());
        attachToSend.setSource(getSource());
        attachToSend.setTarget(getTarget());
        attachToSend.setSndSettleMode(getSendingSettlementMode());
        attachToSend.setRcvSettleMode(getReceivingSettlementMode());
        attachToSend.setUnsettled(getLocalUnsettled());
        attachToSend.setProperties(_properties);
        attachToSend.setOfferedCapabilities(_capabilities);

        if (getRole() == Role.SENDER)
        {
            attachToSend.setInitialDeliveryCount(_deliveryCount.unsignedIntegerValue());
        }
        else
        {
            final long maxMessageSize = getSession().getConnection().getMaxMessageSize();
            if (maxMessageSize != Long.MAX_VALUE)
            {
                attachToSend.setMaxMessageSize(UnsignedLong.valueOf(maxMessageSize));
            }
        }

        attachToSend = handleOversizedUnsettledMapIfNecessary(attachToSend);

        switch (_state)
        {
            case DETACHED:
                _state = State.ATTACH_SENT;
                break;
            case ATTACH_RECVD:
                _state = State.ATTACHED;
                break;
            default:
                throw new UnsupportedOperationException(_state.toString());
        }

        getSession().sendAttach(attachToSend);
    }

    private Attach handleOversizedUnsettledMapIfNecessary(final Attach attachToSend)
    {
        final Session_1_0 session = getSession();
        final AMQPConnection_1_0<?> connection = session.getConnection();
        final AMQPDescribedTypeRegistry describedTypeRegistry = connection.getDescribedTypeRegistry();
        final ValueWriter<Attach> valueWriter = describedTypeRegistry.getValueWriter(attachToSend);
        final int maxFrameSize = connection.getMaxFrameSize();

        if (valueWriter.getEncodedSize() + FRAME_HEADER_SIZE <= maxFrameSize)
        {
            _localIncompleteUnsettled = false;
            return attachToSend;
        }

        _localIncompleteUnsettled = true;
        attachToSend.setIncompleteUnsettled(true);

        Map<Binary, DeliveryState> localUnsettledMap = attachToSend.getUnsettled();
        if (localUnsettledMap == null)
        {
            localUnsettledMap = Collections.emptyMap();
        }

        final List<Map.Entry<Binary, DeliveryState>> entries = new ArrayList<>(localUnsettledMap.entrySet());
        int lowIndex = 0;
        int highIndex = entries.size();
        int currentIndex = (highIndex - lowIndex) / 2;
        int oldIndex;
        HashMap<Binary, DeliveryState> unsettledMap = null;
        int totalSize;

        final HashMap<Binary, DeliveryState> partialUnsettledMap = new HashMap<>(highIndex);
        do
        {
            partialUnsettledMap.clear();
            for (int i = 0; i < currentIndex; ++i)
            {
                final Map.Entry<Binary, DeliveryState> entry = entries.get(i);
                partialUnsettledMap.put(entry.getKey(), entry.getValue());
            }
            attachToSend.setUnsettled(partialUnsettledMap);

            totalSize = valueWriter.getEncodedSize() + FRAME_HEADER_SIZE;

            if (totalSize > maxFrameSize)
            {
                highIndex = currentIndex;
            }
            else if (totalSize < maxFrameSize)
            {
                lowIndex = currentIndex;
                unsettledMap = partialUnsettledMap;
            }
            else
            {
                unsettledMap = new HashMap<>(partialUnsettledMap);
                break;
            }

            oldIndex = currentIndex;
            currentIndex = lowIndex + (highIndex - lowIndex) / 2;
        }
        while (oldIndex != currentIndex);

        if (unsettledMap == null || unsettledMap.isEmpty())
        {
            final End endWithError = new End();
            endWithError.setError(new Error(AmqpError.FRAME_SIZE_TOO_SMALL, "Cannot fit a single unsettled delivery into Attach frame."));
            session.end(endWithError);
        }

        attachToSend.setUnsettled(unsettledMap);

        return attachToSend;
    }

    public void detach()
    {
        detach(null, false);
    }

    public void close()
    {
        detach(null, true);
    }

    public void detach(final Error error)
    {
        detach(error, false);
    }

    @Override
    public void close(final Error error)
    {
        detach(error, true);
    }

    protected void detach(final Error error, final boolean close)
    {
        final Session_1_0 session = getSession();
        if (error != null && !session.isSyntheticError(error))
        {
            _errored = true;
        }

        //TODO: QPID-7954: improve detach
        switch (_state)
        {
            case ATTACHED:
                _state = State.DETACH_SENT;
                break;
            case DETACH_RECVD:
                _state = State.DETACHED;
                break;
            default:
                // "silent link stealing": If a link is attached on a different connection (i.e., it is "stolen") then
                //  the spec say we must close the link with a link:stolen error. This only makes sense if the link is
                //  attached at the time it is stolen. If it is detached we call it "silent link stealing" and simply
                //  disassociate the link from the session so that the new connection can use it.
                if (close)
                {
                    session.dissociateEndpoint(this);
                    destroy();
                    _link.linkClosed();
                }
                return;
        }

        if (session.getSessionState() != SessionState.END_RECVD && !session.isEnded())
        {
            final Detach detach = new Detach();
            detach.setHandle(getLocalHandle());
            if (close)
            {
                detach.setClosed(close);
            }
            detach.setError(error);

            session.sendDetach(detach);
        }

        if (close)
        {
            destroy();
            _link.linkClosed();
        }
        setLocalHandle(null);
    }

    public void sendFlowConditional()
    {
        if (_lastSentCreditLimit != null)
        {
            if (_stoppedUpdated)
            {
                sendFlow(false);
                _stoppedUpdated = false;
            }
            else
            {
                final UnsignedInteger clientsCredit = _lastSentCreditLimit.subtract(_deliveryCount.unsignedIntegerValue());

                // client has used up over half their credit allowance ?
                final boolean sendFlow = _linkCredit.subtract(clientsCredit).compareTo(clientsCredit) >= 0;
                if (sendFlow)
                {
                    sendFlow(false);
                }
                else
                {
                    getSession().sendFlowConditional();
                }
            }
        }
        else
        {
            sendFlow(false);
        }
    }

    @Override
    public void sendFlow()
    {
        sendFlow(false);
    }

    private void sendFlow(final boolean echo)
    {
        if (_state == State.ATTACHED || _state == State.ATTACH_SENT)
        {
            final Flow flow = new Flow();
            final UnsignedInteger deliveryCount = _deliveryCount.unsignedIntegerValue();
            flow.setDeliveryCount(deliveryCount);
            flow.setEcho(echo);
            if (_stopped)
            {
                flow.setLinkCredit(UnsignedInteger.ZERO);
                flow.setDrain(true);
                _lastSentCreditLimit = deliveryCount;
            }
            else
            {
                flow.setLinkCredit(_linkCredit);
                _lastSentCreditLimit = _linkCredit.add(deliveryCount);
                flow.setDrain(_drain);
            }
            flow.setAvailable(_available);
            flow.setHandle(getLocalHandle());
            getSession().sendFlow(flow);
        }
    }

    protected Link_1_0<S, T> getLink()
    {
        return _link;
    }

    @Override
    public SenderSettleMode getSendingSettlementMode()
    {
        return _sendingSettlementMode;
    }

    @Override
    public ReceiverSettleMode getReceivingSettlementMode()
    {
        return _receivingSettlementMode;
    }

    public List<Symbol> getCapabilities()
    {
        return _capabilities == null ? null : Collections.unmodifiableList(Arrays.asList(_capabilities));
    }

    public void setCapabilities(Collection<Symbol> capabilities)
    {
        _capabilities = capabilities == null ? null : capabilities.toArray(new Symbol[0]);
    }

    public boolean isErrored()
    {
        return _errored;
    }

    @Override
    public String toString()
    {
        return "LinkEndpoint{" +
               "_name='" + getLinkName() + '\'' +
               ", _session=" + _session +
               ", _state=" + _state +
               ", _role=" + getRole() +
               ", _source=" + getSource() +
               ", _target=" + getTarget() +
               ", _transferCount=" + _deliveryCount +
               ", _linkCredit=" + _linkCredit +
               ", _available=" + _available +
               ", _drain=" + _drain +
               ", _localHandle=" + _localHandle +
               '}';
    }
}
