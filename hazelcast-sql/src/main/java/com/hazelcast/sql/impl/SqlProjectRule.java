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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

public class SqlProjectRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new SqlProjectRule();

    public SqlProjectRule() {
        super(LogicalProject.class, Convention.NONE, SqlRel.CONVENTION, SqlProjectRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        assert call.rels.length == 1;
        LogicalProject project = call.rel(0);

        assert project.getInputs().size() == 1;
        if (project.getInput().getConvention() != SqlRel.CONVENTION) {
            return false;
        }

        // TODO: proper fields detection
        for (RexNode node : project.getProjects()) {
            if (!(node instanceof RexInputRef)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalProject project = (LogicalProject) rel;
        final RelTraitSet traitSet = project.getTraitSet().replace(SqlRel.CONVENTION);
        // TODO: convert(project.getInput(), SqlRel.CONVENTION), do we need it?
        return new SqlProject(project.getCluster(), traitSet, project.getInput(), project.getProjects(), project.getRowType());
    }

}
