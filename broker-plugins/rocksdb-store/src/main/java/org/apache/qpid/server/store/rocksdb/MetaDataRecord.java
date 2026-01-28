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

import org.apache.qpid.server.store.StorableMessageMetaData;

/**
 * Holds decoded metadata details.
 *
 * Thread-safety: immutable.
 */
final class MetaDataRecord
{
    private final StorableMessageMetaData _metaData;
    private final boolean _chunked;
    private final int _chunkSize;

    MetaDataRecord(final StorableMessageMetaData metaData,
                   final boolean chunked,
                   final int chunkSize)
    {
        _metaData = metaData;
        _chunked = chunked;
        _chunkSize = chunkSize;
    }

    StorableMessageMetaData getMetaData()
    {
        return _metaData;
    }

    boolean isChunked()
    {
        return _chunked;
    }

    int getChunkSize()
    {
        return _chunkSize;
    }
}
