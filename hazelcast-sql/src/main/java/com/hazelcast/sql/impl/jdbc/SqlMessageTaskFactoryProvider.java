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
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.remote.Handler;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.ProtobufHandler;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.Service;

import java.security.Permission;
import java.sql.SQLException;
import java.util.Properties;

public class SqlMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    private final MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];

    public SqlMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        Node node = ((NodeEngineImpl) nodeEngine).getNode();

        Properties info = new Properties();
        info.put("hazelcast-node-engine", nodeEngine);
        Meta meta;
        try {
            meta = new JdbcMeta("jdbc:hazelcast-local:", info);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        MetricsSystem metricsSystem = NoopMetricsSystem.getInstance();
        Service service = new LocalService(meta, metricsSystem);
        Handler<byte[]> handler = new ProtobufHandler(service, new ProtobufTranslationImpl(), metricsSystem);

        factories[Short.MAX_VALUE - 10] = (request, connection) -> new AbstractMessageTask<byte[]>(request, node, connection) {
            @Override
            protected byte[] decodeClientMessage(ClientMessage clientMessage) {
                return SqlClientMessages.decode(clientMessage);
            }

            @Override
            protected ClientMessage encodeResponse(Object response) {
                return SqlClientMessages.encodeResponse((byte[]) response);
            }

            @Override
            protected void processMessage() {
                Handler.HandlerResponse<byte[]> handlerResponse = handler.apply(parameters);
                // looks like status codes are meaningful for JSON protocol only
//                if (handlerResponse.getStatusCode() != Handler.HTTP_OK) {
//                    throw new SQLException("handler reported error: " + handlerResponse.getStatusCode());
//                }
                sendResponse(handlerResponse.getResponse());
            }

            @Override
            public String getServiceName() {
                return MapService.SERVICE_NAME;
            }

            @Override
            public Permission getRequiredPermission() {
                return new MapPermission("*", ActionConstants.ACTION_READ);
            }

            @Override
            public String getDistributedObjectName() {
                return "*";
            }

            @Override
            public String getMethodName() {
                return "entrySet";
            }

            @Override
            public Object[] getParameters() {
                return new Object[0];
            }
        };
    }

    @Override
    public MessageTaskFactory[] getFactories() {
        return factories;
    }

}
