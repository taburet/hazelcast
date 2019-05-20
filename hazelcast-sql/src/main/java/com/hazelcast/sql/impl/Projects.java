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

import com.hazelcast.projection.Projections;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.lang.reflect.Method;
import java.util.List;

public final class Projects {

    private static final Method SINGLE_ATTRIBUTE = projection("singleAttribute", String.class);
    private static final Method MULTI_ATTRIBUTE = projection("multiAttribute", String[].class);

    public static boolean isSupported(Project project) {
        // TODO: proper fields detection
        for (RexNode node : project.getProjects()) {
            if (!(node instanceof RexInputRef)) {
                return false;
            }
        }

        return true;
    }

    public static Expression representAsExpression(Project project) {
        if (project == null) {
            return Expressions.constant(null);
        }

        List<String> fieldNames = project.getRowType().getFieldNames();

        if (fieldNames.size() == 1) {
            return Expressions.call(SINGLE_ATTRIBUTE, Expressions.constant(fieldNames.get(0)));
        } else {
            Expression[] attributePaths = new Expression[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); ++i) {
                attributePaths[i] = Expressions.constant(fieldNames.get(i));
            }
            return Expressions.call(MULTI_ATTRIBUTE, Expressions.newArrayInit(String.class, attributePaths));
        }
    }

    private static Method projection(String name, Class... args) {
        Method method;
        try {
            method = Projections.class.getMethod(name, args);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
        method.setAccessible(true);
        return method;
    }

    private Projects() {
    }

}
