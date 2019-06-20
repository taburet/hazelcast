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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.InternalProperty;
import org.apache.calcite.avatica.remote.AvaticaHttpClient;
import org.apache.calcite.avatica.remote.AvaticaHttpClientFactory;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.KerberosConnection;
import org.apache.calcite.avatica.util.Casing;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class RemoteJdbcDriver extends Driver {

    static {
        new RemoteJdbcDriver().register();
    }

    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:hazelcast-remote:";
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return new DriverVersion("Hazelcast Remote JDBC Driver", "0.1", "Hazelcast",
                BuildInfoProvider.getBuildInfo().getVersion(), false, 0, 1, 0, 1);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        // TODO: this doesn't work
        info.put(InternalProperty.UNQUOTED_CASING, Casing.UNCHANGED);
        info.put(InternalProperty.QUOTED_CASING, Casing.UNCHANGED);
        info.put(InternalProperty.CASE_SENSITIVE, Boolean.TRUE);

        URI uri;
        try {
            uri = new URI(new URI(url).getRawSchemeSpecificPart());
        } catch (URISyntaxException e) {
            throw new SQLException(e);
        }

        info.put(BuiltInConnectionProperty.HTTP_CLIENT_FACTORY.camelName(), HttpClientFactory.class.getName());
        info.put(BuiltInConnectionProperty.SERIALIZATION.camelName(), "protobuf");
        info.put(BuiltInConnectionProperty.URL.camelName(), "http://" + uri.getHost() + ":" + uri.getPort());

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress(uri.getHost() + ":" + uri.getPort());
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        info.put("hazelcast-client", client);

        url = getConnectStringPrefix();
        try {
            return super.connect(url, info);
        } catch (Throwable throwable) {
            client.shutdown();
            throw throwable;
        }
    }

    @Override
    protected Handler createHandler() {
        Handler handler = super.createHandler();
        return new Handler() {
            @Override
            public void onConnectionInit(AvaticaConnection connection) throws SQLException {
                handler.onConnectionInit(connection);
            }

            @Override
            public void onConnectionClose(AvaticaConnection connection) {
                handler.onConnectionClose(connection);

                HazelcastInstance client;
                try {
                    // XXX: invent a better way of passing parameters,
                    ConnectionConfig config = connection.config();
                    Field propertiesField = config.getClass().getDeclaredField("properties");
                    propertiesField.setAccessible(true);
                    Properties properties = (Properties) propertiesField.get(config);
                    client = ((HazelcastInstance) properties.get("hazelcast-client"));
                } catch (IllegalAccessException | NoSuchFieldException e) {
                    throw new RuntimeException(e);
                }

                client.shutdown();
            }

            @Override
            public void onStatementExecute(AvaticaStatement statement, ResultSink resultSink) {
                handler.onStatementExecute(statement, resultSink);
            }

            @Override
            public void onStatementClose(AvaticaStatement statement) {
                handler.onStatementClose(statement);
            }
        };
    }

    public static class HttpClientFactory implements AvaticaHttpClientFactory {

        @Override
        public AvaticaHttpClient getClient(URL url, ConnectionConfig config, KerberosConnection kerberosUtil) {
            HazelcastClientInstanceImpl client;
            try {
                // XXX: invent a better way of passing parameters
                Field propertiesField = config.getClass().getDeclaredField("properties");
                propertiesField.setAccessible(true);
                Properties properties = (Properties) propertiesField.get(config);
                client = ((HazelcastClientProxy) properties.get("hazelcast-client")).client;
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw new RuntimeException(e);
            }

            return request -> {
                try {
                    ClientMessage message = SqlClientMessages.encodeRequest(request);
                    ClientMessage response = new ClientInvocation(client, message, null,
                            client.getConnectionManager().getOwnerConnection()).invoke().get();
                    return SqlClientMessages.decode(response);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            };
        }

    }

}
