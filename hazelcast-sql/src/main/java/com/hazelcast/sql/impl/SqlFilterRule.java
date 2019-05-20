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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;

public class SqlFilterRule extends ConverterRule /* RelOptRule */ {

    public static final RelOptRule INSTANCE = new SqlFilterRule();

    public SqlFilterRule() {
        super(LogicalFilter.class, Convention.NONE, SqlRel.CONVENTION, SqlFilterRule.class.getSimpleName());
//        super(operand(LogicalFilter.class, operand(SqlRel.class, any())), SqlFilterRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Filter filter = call.rel(0);

        // TODO: this is wrong and doesn't work, we should not travers the tree manually, use proper matching!
//        if (SqlRel.findInputOf(filter, SqlFilter.class) != null) {
//            return false;
//        }

        return Filters.isSupported(filter);
    }

    @Override
    public RelNode convert(RelNode rel) {
        Filter filter = (LogicalFilter) rel;

        RelTraitSet traitSet = filter.getTraitSet().replace(SqlRel.CONVENTION);
        return new SqlFilter(filter.getCluster(), traitSet, convert(filter.getInput(), SqlRel.CONVENTION), filter.getCondition());
    }

//    @Override
//    public void onMatch(RelOptRuleCall call) {
//        LogicalFilter filter = call.rel(0);
//
//        RelTraitSet traitSet = filter.getTraitSet().replace(SqlRel.CONVENTION);
//        SqlFilter newFilter = new SqlFilter(filter.getCluster(), traitSet, convert(filter.getInput(), SqlRel.CONVENTION),
//                filter.getCondition());
//
//        call.transformTo(newFilter);
//
//    }

}
