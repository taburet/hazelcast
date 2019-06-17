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

package com.hazelcast.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.impl.SqlContext;
import com.hazelcast.sql.impl.SqlPrepare;
import com.hazelcast.sql.impl.SqlTranslatableTable;
import com.hazelcast.sql.pojos.Person;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;

public final class HazelcastSql {

    private final SqlPrepare sqlPrepare = new SqlPrepare();

    private final CalciteSchema schema = CalciteSchema.createRootSchema(true);

    private final JavaTypeFactory javaTypeFactory = new JavaTypeFactoryImpl();

    private HazelcastSql(HazelcastInstance instance) {
        // TODO: real schema support, where to get it?
        // TODO: make __key field to be a pseudo field

//        schema.add("persons",
//                new SqlScannableTable(Person.class, javaTypeFactory.createStructType(Person.class), instance.getMap
//                ("persons")));

        schema.add("persons", new SqlTranslatableTable(Person.class, javaTypeFactory.createStructType(Person.class),
                instance.getMap("persons")));
    }

    public static HazelcastSql createFor(HazelcastInstance instance) {
        return new HazelcastSql(instance);
    }

    public Enumerable<Object> query(String query) {
        SqlContext context = new SqlContext(javaTypeFactory, schema);

        CalcitePrepare.Dummy.push(context);
        try {
//        CalcitePrepare.ParseResult parseResult = sqlPrepare.parse(context, query);
//        System.out.println(parseResult.sqlNode);

//        CalcitePrepare.ConvertResult convertResult = sqlPrepare.convert(context, query);
//        System.out.println(convertResult.root);

            CalcitePrepare.CalciteSignature<Object> calciteSignature =
                    sqlPrepare.prepareSql(context, CalcitePrepare.Query.of(query), Object[].class, -1);
            context.setInternalParameters(calciteSignature.internalParameters);
            return calciteSignature.enumerable(context.getDataContext());
        } finally {
            CalcitePrepare.Dummy.pop(context);
        }
    }

    public String explain(String query) {
        Enumerable<Object> result = query("explain plan including all attributes for " + query);
        return (String) result.first();
    }

    public String explainLogical(String query) {
        SqlContext context = new SqlContext(javaTypeFactory, schema);

        CalcitePrepare.Dummy.push(context);
        try {
            CalcitePrepare.ConvertResult convertResult = sqlPrepare.convert(context, query);
            return RelOptUtil.dumpPlan("", convertResult.root.rel, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES);
        } finally {
            CalcitePrepare.Dummy.pop(context);
        }
    }

}
