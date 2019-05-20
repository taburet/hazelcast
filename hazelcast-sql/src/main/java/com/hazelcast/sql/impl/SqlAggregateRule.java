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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class SqlAggregateRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new SqlAggregateRule();

    public SqlAggregateRule() {
        super(LogicalAggregate.class, Convention.NONE, SqlRel.CONVENTION, SqlAggregateRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Aggregate aggregate = call.rel(0);

        // TODO: this is wrong and doesn't work, we should not travers the tree manually, use proper matching!
//        if (SqlRel.findInputOf(aggregate, SqlAggregate.class) != null
//                || SqlRel.findInputOf(aggregate, SqlProject.class) != null) {
//            return false;
//        }

        return Aggregates.isSupported(aggregate);
    }

    @Override
    public RelNode convert(RelNode rel) {
        Aggregate aggregate = (LogicalAggregate) rel;

        RelTraitSet traitSet = aggregate.getTraitSet().replace(SqlRel.CONVENTION);
        return new SqlAggregate(aggregate.getCluster(), traitSet, convert(aggregate.getInput(), SqlRel.CONVENTION),
                aggregate.indicator, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
    }

}
