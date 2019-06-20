/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.jdbc;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;

public final class SqlClientMessages {

    public static ClientMessage encodeRequest(byte[] bytes) {
        ClientMessage message = ClientMessage.createForEncode(ClientMessage.HEADER_SIZE + ParameterUtil.calculateDataSize(bytes));
        message.setMessageType(Short.MAX_VALUE - 10);
        message.setRetryable(false);
        message.setAcquiresResource(false);
        message.setOperationName("sql");
        message.set(bytes);
        message.updateFrameLength();
        return message;
    }

    public static ClientMessage encodeResponse(byte[] bytes) {
        ClientMessage message = ClientMessage.createForEncode(ClientMessage.HEADER_SIZE + ParameterUtil.calculateDataSize(bytes));
        message.setMessageType(Short.MAX_VALUE - 10);
        message.set(bytes);
        message.updateFrameLength();
        return message;
    }

    public static byte[] decode(ClientMessage message) {
        return message.getByteArray();
    }

    private SqlClientMessages() {
    }

}
