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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.sql.impl.SqlPrepare;
import com.hazelcast.sql.impl.SqlTable;
import com.hazelcast.sql.pojos.Person;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LocalJdbcDriver extends Driver {

    static {
        new LocalJdbcDriver().register();
    }

    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:hazelcast-local:";
    }

    @Override
    protected Function0<CalcitePrepare> createPrepareFactory() {
        return SqlPrepare::new;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        info.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        info.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        info.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());

        CalciteConnection connection = (CalciteConnection) super.connect(url, info);

        NodeEngine nodeEngine = (NodeEngine) info.get("hazelcast-node-engine");
        HazelcastInstance instance = nodeEngine.getHazelcastInstance();

        // TODO proper schema support
        SchemaImpl schema = new SchemaImpl();
        schema.addTable("persons",
                new SqlTable(connection.getTypeFactory().createStructType(Person.class), instance.getMap("persons")));
        connection.getRootSchema().add("hazelcast", schema);
        connection.setSchema("hazelcast");

        return connection;
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return new DriverVersion("Hazelcast Local JDBC Driver", "0.1", "Hazelcast", BuildInfoProvider.getBuildInfo().getVersion(),
                false, 0, 1, 0, 1);
    }

    private static class SchemaImpl extends AbstractSchema {

        private final Map<String, Table> tableMap = new HashMap<>();

        private final Map<String, Table> unmodifiableTableMap = Collections.unmodifiableMap(tableMap);

        public void addTable(String name, Table table) {
            tableMap.put(name, table);
        }

        @Override
        protected Map<String, Table> getTableMap() {
            return unmodifiableTableMap;
        }

    }

}
