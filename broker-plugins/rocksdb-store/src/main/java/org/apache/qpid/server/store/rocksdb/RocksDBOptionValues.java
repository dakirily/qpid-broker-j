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

package org.apache.qpid.server.store.rocksdb;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.TxnDBWritePolicy;

/**
 * Parses RocksDB option values from strings.
 * <br>
 * Thread-safety: safe for concurrent access.
 */
public final class RocksDBOptionValues
{
    /**
     * Prevents instantiation.
     */
    private RocksDBOptionValues()
    {
    }

    /**
     * Parses a compaction style from string.
     *
     * @param value compaction style name.
     *
     * @return the compaction style.
     *
     * @throws IllegalArgumentException for an unknown value.
     */
    public static CompactionStyle parseCompactionStyle(final String value)
    {
        for (CompactionStyle style : CompactionStyle.values())
        {
            if (style.name().equalsIgnoreCase(value))
            {
                return style;
            }
        }
        throw new IllegalArgumentException("Unknown compaction style: " + value);
    }

    /**
     * Parses a compression type from string.
     *
     * @param value compression type name.
     *
     * @return the compression type.
     *
     * @throws IllegalArgumentException for an unknown value.
     */
    public static CompressionType parseCompressionType(final String value)
    {
        for (CompressionType type : CompressionType.values())
        {
            if (type.name().equalsIgnoreCase(value))
            {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown compression type: " + value);
    }

    /**
     * Parses a TransactionDB write policy from string.
     *
     * @param value write policy name.
     *
     * @return the write policy.
     *
     * @throws IllegalArgumentException for an unknown value.
     */
    public static TxnDBWritePolicy parseTxnDbWritePolicy(final String value)
    {
        for (TxnDBWritePolicy policy : TxnDBWritePolicy.values())
        {
            if (policy.name().equalsIgnoreCase(value))
            {
                return policy;
            }
        }
        throw new IllegalArgumentException("Unknown transaction write policy: " + value);
    }
}
