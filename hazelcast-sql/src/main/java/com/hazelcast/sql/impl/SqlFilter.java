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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class SqlFilter extends Filter implements SqlRel {

    public SqlFilter(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode condition) {
        super(cluster, traits, input, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new SqlFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
//        TableScan tableScan = SqlRel.findInputOf(this, SqlTableScan.class);
//        SqlRel input = SqlRel.findInputOf(this, SqlRel.class);
//        assert tableScan != null && input != null;
//
//        if (input == tableScan) {
//            RelOptCost cost = input.computeSelfCost(planner, mq);
//            return planner.getCostFactory().makeCost(-cost.getRows() * 0.75, 0.0, 0.0);
//        } else {
//            return planner.getCostFactory().makeZeroCost();
//        }

        return planner.getCostFactory().makeZeroCost();

        //return super.computeSelfCost(planner, mq).multiplyBy(0.1);

//        SqlRel input = SqlRel.findInputOf(this, SqlRel.class);
//        assert input != null;
//
//        RelOptCost cost = input.computeSelfCost(planner, mq);
//        System.out.println("Filter: " + input);
//
//        double rows = -cost.getRows() * 0.75;
//        double io = -cost.getIo() * 0.75;
//        return planner.getCostFactory().makeCost(rows, 0.0, io);
    }

    @Override
    public void implement(SqlImplementation implementation) {
        ((SqlRel) getInput()).implement(implementation);
        implementation.filter = this;
    }

}
