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

import com.hazelcast.aggregation.Aggregators;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;

import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.Set;

public final class Aggregates {

    private static final Set<SqlKind> SUPPORTED_AGGREGATIONS =
            EnumSet.of(SqlKind.COUNT, SqlKind.MIN, SqlKind.MAX, SqlKind.AVG, SqlKind.SUM);

    private static final Method DISTINCT = aggregator("distinct", String.class);

    // TODO: support type-specific aggregators (?)
    private static final Method COUNT = aggregator("count", String.class);
    private static final Method STAR_COUNT = aggregator("count");
    private static final Method MAX = aggregator("comparableMax", String.class);
    private static final Method MIN = aggregator("comparableMin", String.class);
    private static final Method AVG = aggregator("numberAvg", String.class);
    private static final Method SUM = aggregator("floatingPointSum", String.class);

    public static boolean isSupported(Aggregate aggregate) {
        // TODO: there is no proper input provided by Calcite during optimization (bug?), how to check for fields?

        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            return false;
        }

        if (aggregate.getAggCallList().isEmpty() && aggregate.getGroupSets().size() == 1
                && aggregate.getGroupSet().cardinality() == 1 && aggregate.getRowType().getFieldCount() == 1) {
            return true;
        }

        if (aggregate.getAggCallList().size() == 1 && aggregate.getGroupSets().size() == 1 && aggregate.getGroupSet().isEmpty()
                && aggregate.getRowType().getFieldCount() == 1) {
            // TODO: why aggregate.getGroupSets().size() == 1? is it a bug?
            return isSupported(aggregate.getAggCallList().get(0));
        }

        return false;
    }

    public static Expression representAsExpression(Aggregate aggregate) {
        if (aggregate == null) {
            return Expressions.constant(null);
        }

        if (aggregate.getAggCallList().isEmpty() && aggregate.getGroupSets().size() == 1
                && aggregate.getGroupSet().cardinality() == 1 && aggregate.getRowType().getFieldCount() == 1) {
            String fieldName =
                    aggregate.getInput().getRowType().getFieldList().get(aggregate.getGroupSet().nextSetBit(0)).getName();
            return Expressions.call(DISTINCT, Expressions.constant(fieldName));
        }

        if (aggregate.getAggCallList().size() == 1 && aggregate.getGroupSets().size() == 1 && aggregate.getGroupSet().isEmpty()
                && aggregate.getRowType().getFieldCount() == 1) {
            // TODO: why aggregate.getGroupSets().size() == 1? is it a bug?

            AggregateCall call = aggregate.getAggCallList().get(0);
            String fieldName = call.getArgList().size() == 0 ? null : aggregate.getInput().getRowType().getFieldList().get(
                    call.getArgList().get(0)).getName();

            switch (call.getAggregation().kind) {
                case COUNT:
                    return fieldName == null ? Expressions.call(STAR_COUNT) : Expressions.call(COUNT,
                            Expressions.constant(fieldName));
                case MAX:
                    assert fieldName != null;
                    return Expressions.call(MAX, Expressions.constant(fieldName));
                case MIN:
                    assert fieldName != null;
                    return Expressions.call(MIN, Expressions.constant(fieldName));
                case AVG:
                    assert fieldName != null;
                    return Expressions.call(AVG, Expressions.constant(fieldName));
                case SUM:
                    assert fieldName != null;
                    return Expressions.call(SUM, Expressions.constant(fieldName));
            }
        }

        throw new IllegalStateException();
    }

    private static boolean isSupported(AggregateCall call) {
        if (call.getArgList().size() > 1 || call.isDistinct()) {
            return false;
        }

        return call.getAggregation().kind.belongsTo(SUPPORTED_AGGREGATIONS);
    }

    private static Method aggregator(String name, Class... args) {
        Method method;
        try {
            method = Aggregators.class.getMethod(name, args);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
        method.setAccessible(true);
        return method;
    }

    private Aggregates() {
    }

}
