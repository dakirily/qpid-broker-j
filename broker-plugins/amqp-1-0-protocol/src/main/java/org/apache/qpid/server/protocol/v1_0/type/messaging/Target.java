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

package org.apache.qpid.server.protocol.v1_0.type.messaging;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import org.apache.qpid.server.protocol.v1_0.CompositeType;
import org.apache.qpid.server.protocol.v1_0.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;

@CompositeType(symbolicDescriptor = "amqp:target:list", numericDescriptor = 0x0000000000000029L)
public class Target implements BaseTarget, Terminus
{
    @CompositeTypeField(index = 0)
    private String _address;

    @CompositeTypeField(index = 1)
    private TerminusDurability _durable;

    @CompositeTypeField(index = 2)
    private TerminusExpiryPolicy _expiryPolicy;

    @CompositeTypeField(index = 3)
    private UnsignedInteger _timeout;

    @CompositeTypeField(index = 4)
    private Boolean _dynamic;

    @CompositeTypeField(index = 5, deserializationConverter = "org.apache.qpid.server.protocol.v1_0.DeserializationFactories.convertToNodeProperties")
    private Map<Symbol, Object> _dynamicNodeProperties;

    @CompositeTypeField(index = 6)
    private Symbol[] _capabilities;

    public String getAddress()
    {
        return _address;
    }

    public void setAddress(final String address)
    {
        _address = address;
    }

    public TerminusDurability getDurable()
    {
        return _durable;
    }

    public void setDurable(final TerminusDurability durable)
    {
        _durable = durable;
    }

    public TerminusExpiryPolicy getExpiryPolicy()
    {
        return _expiryPolicy;
    }

    public void setExpiryPolicy(final TerminusExpiryPolicy expiryPolicy)
    {
        _expiryPolicy = expiryPolicy;
    }

    public UnsignedInteger getTimeout()
    {
        return _timeout;
    }

    public void setTimeout(final UnsignedInteger timeout)
    {
        _timeout = timeout;
    }

    public Boolean getDynamic()
    {
        return _dynamic;
    }

    public void setDynamic(final Boolean dynamic)
    {
        _dynamic = dynamic;
    }

    public Map<Symbol, Object> getDynamicNodeProperties()
    {
        return _dynamicNodeProperties;
    }

    public void setDynamicNodeProperties(final Map<Symbol, Object> dynamicNodeProperties)
    {
        _dynamicNodeProperties = dynamicNodeProperties;
    }

    public Symbol[] getCapabilities()
    {
        return _capabilities;
    }

    public void setCapabilities(final Symbol[] capabilities)
    {
        _capabilities = capabilities;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final Target target = (Target) o;

        if (!Objects.equals(_address, target._address))
        {
            return false;
        }
        if (!Objects.equals(_durable, target._durable))
        {
            return false;
        }
        if (!Objects.equals(_expiryPolicy, target._expiryPolicy))
        {
            return false;
        }
        if (!Objects.equals(_timeout, target._timeout))
        {
            return false;
        }
        if (!Objects.equals(_dynamic, target._dynamic))
        {
            return false;
        }
        if (!Objects.equals(_dynamicNodeProperties, target._dynamicNodeProperties))
        {
            return false;
        }

        return Arrays.equals(_capabilities, target._capabilities);
    }

    @Override
    public int hashCode()
    {
        int result = _address != null ? _address.hashCode() : 0;
        result = 31 * result + (_durable != null ? _durable.hashCode() : 0);
        result = 31 * result + (_expiryPolicy != null ? _expiryPolicy.hashCode() : 0);
        result = 31 * result + (_timeout != null ? _timeout.hashCode() : 0);
        result = 31 * result + (_dynamic != null ? _dynamic.hashCode() : 0);
        result = 31 * result + (_dynamicNodeProperties != null ? _dynamicNodeProperties.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(_capabilities);
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder builder = new StringBuilder("Target{");
        final int origLength = builder.length();

        if (_address != null)
        {
            builder.append("address=").append(_address);
        }

        if (_durable != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("durable=").append(_durable);
        }

        if (_expiryPolicy != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("expiryPolicy=").append(_expiryPolicy);
        }

        if (_timeout != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("timeout=").append(_timeout);
        }

        if (_dynamic != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("dynamic=").append(_dynamic);
        }

        if (_dynamicNodeProperties != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("dynamicNodeProperties=").append(_dynamicNodeProperties);
        }

        if (_capabilities != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("capabilities=").append(Arrays.toString(_capabilities));
        }

        builder.append('}');
        return builder.toString();
    }
}
