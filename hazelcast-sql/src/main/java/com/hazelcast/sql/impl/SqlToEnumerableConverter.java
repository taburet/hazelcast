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

package com.hazelcast.sql.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.runtime.Hook;

import java.lang.reflect.Method;
import java.util.List;

public class SqlToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    private static final Method QUERY_METHOD =
            Types.lookupMethod(SqlTranslatableTable.QueryableImpl.class, "query", Aggregator.class, Projection.class,
                    Predicate.class);

    public SqlToEnumerableConverter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traitSet, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new SqlToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        SqlImplementation implementation = new SqlImplementation();
        ((SqlRel) getInput()).implement(implementation);
        System.out.println(implementation);

        final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));

        BlockBuilder blockBuilder = new BlockBuilder().append(
                Expressions.call(implementation.table.getExpression(SqlTranslatableTable.QueryableImpl.class), QUERY_METHOD,
                        Aggregates.representAsExpression(implementation.aggregate),
                        Projects.representAsExpression(implementation.project),
                        Filters.representAsExpression(implementation.filter)));

        Hook.QUERY_PLAN.run(implementation);

        return implementor.result(physType, blockBuilder.toBlock());
    }

}
