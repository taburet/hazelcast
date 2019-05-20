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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;

public class SqlProjectRule extends ConverterRule /*RelOptRule*/ {

    public static final RelOptRule INSTANCE = new SqlProjectRule();

    public SqlProjectRule() {
        super(LogicalProject.class, Convention.NONE, SqlRel.CONVENTION, SqlProjectRule.class.getSimpleName());
        //super(operand(LogicalProject.class, operand(SqlRel.class, any())), SqlProjectRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Project project = call.rel(0);

        // TODO: this is wrong and doesn't work, we should not travers the tree manually, use proper matching!
//        if (SqlRel.findInputOf(project, SqlProject.class) != null || SqlRel.findInputOf(project, SqlAggregate.class) != null) {
//            return false;
//        }

        return Projects.isSupported(project);
    }

    @Override
    public RelNode convert(RelNode rel) {
        Project project = (LogicalProject) rel;

        RelTraitSet traitSet = project.getTraitSet().replace(SqlRel.CONVENTION);
        return new SqlProject(project.getCluster(), traitSet, convert(project.getInput(), SqlRel.CONVENTION),
                project.getProjects(), project.getRowType());
    }

    //    @Override
//    public void onMatch(RelOptRuleCall call) {
//        LogicalProject project = call.rel(0);
//
//        RelTraitSet traitSet = project.getTraitSet().replace(SqlRel.CONVENTION);
//        SqlProject newProject = new SqlProject(project.getCluster(), traitSet, convert(project.getInput(), SqlRel.CONVENTION),
//                project.getProjects(), project.getRowType());
//
//        call.transformTo(newProject);
//    }

}
