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
package org.apache.qpid.server.protocol.v1_0.delivery;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.qpid.server.protocol.v1_0.LinkEndpoint;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;

public class DeliveryRegistryImpl implements DeliveryRegistry
{
    private final Map<UnsignedInteger, UnsettledDelivery> _deliveries = new ConcurrentHashMap<>();
    private final Map<LinkEndpoint, Map<Binary, UnsignedInteger>> _deliveryIds = new ConcurrentHashMap<>();

    @Override
    public void addDelivery(final UnsignedInteger deliveryId, final UnsettledDelivery unsettledDelivery)
    {
        _deliveries.put(deliveryId, unsettledDelivery);
        if (!_deliveryIds.containsKey(unsettledDelivery.getLinkEndpoint()))
        {
            _deliveryIds.put(unsettledDelivery.getLinkEndpoint(), new ConcurrentHashMap<>());
        }
        _deliveryIds.get(unsettledDelivery.getLinkEndpoint()).put(unsettledDelivery.getDeliveryTag(), deliveryId);
    }

    @Override
    public void removeDelivery(final UnsignedInteger deliveryId)
    {
        final UnsettledDelivery unsettledDelivery = _deliveries.remove(deliveryId);
        if (unsettledDelivery != null)
        {
            final Map<Binary, UnsignedInteger> map = _deliveryIds.get(unsettledDelivery.getLinkEndpoint());
            map.remove(unsettledDelivery.getDeliveryTag());
            if (map.isEmpty())
            {
                _deliveryIds.remove(unsettledDelivery.getLinkEndpoint());
            }
        }
    }

    @Override
    public UnsettledDelivery getDelivery(final UnsignedInteger deliveryId)
    {
        return _deliveries.get(deliveryId);
    }

    @Override
    public void removeDeliveriesForLinkEndpoint(final LinkEndpoint<?, ?> linkEndpoint)
    {
        final Map<Binary, UnsignedInteger> map = _deliveryIds.remove(linkEndpoint);
        if (map != null)
        {
            map.values().forEach(_deliveries::remove);
        }
    }

    @Override
    public UnsignedInteger getDeliveryId(final Binary deliveryTag, final LinkEndpoint<?, ?> linkEndpoint)
    {
        return _deliveryIds.get(linkEndpoint).get(deliveryTag);
    }

    @Override
    public int size()
    {
        return _deliveries.size();
    }
}
