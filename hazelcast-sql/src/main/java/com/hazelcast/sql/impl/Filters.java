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

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public final class Filters {
    // TODO: more operators and types to support

    private static final Set<SqlKind> SUPPORTED_LOGIC = EnumSet.of(SqlKind.AND, SqlKind.OR, SqlKind.NOT);

    private static final Set<SqlKind> SUPPORTED_COMPARISONS =
            EnumSet.of(SqlKind.EQUALS, SqlKind.NOT_EQUALS, SqlKind.LESS_THAN, SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL,
                    SqlKind.LESS_THAN_OR_EQUAL);

    private static final Set<SqlTypeName> SUPPORTED_TYPES =
            EnumSet.of(SqlTypeName.BOOLEAN, SqlTypeName.INTEGER, SqlTypeName.DOUBLE, SqlTypeName.CHAR, SqlTypeName.VARCHAR,
                    SqlTypeName.DECIMAL);

    private static final Method EQUAL = predicate("equal", String.class, Comparable.class);
    private static final Method NOT_EQUAL = predicate("notEqual", String.class, Comparable.class);
    private static final Method LESS_THAN = predicate("lessThan", String.class, Comparable.class);
    private static final Method GREATER_THAN = predicate("greaterThan", String.class, Comparable.class);
    private static final Method LESS_EQUAL = predicate("lessEqual", String.class, Comparable.class);
    private static final Method GREATER_EQUAL = predicate("greaterEqual", String.class, Comparable.class);

    private static final Method AND = predicate("and", Predicate[].class);
    private static final Method OR = predicate("or", Predicate[].class);
    private static final Method NOT = predicate("not", Predicate.class);

    public static boolean isSupported(Filter filter) {
        return isSupported(filter.getCondition());
    }

    private static boolean isSupported(RexNode node) {
        if (isField(node)) {
            // TODO: make sure it's a boolean field ref
            return true;
        }

        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;

            if (call.op.kind.belongsTo(SUPPORTED_COMPARISONS)) {
                List<RexNode> operands = call.getOperands();
                RexLiteral literal;
                if (isField(operands.get(0)) && operands.get(1) instanceof RexLiteral) {
                    literal = (RexLiteral) operands.get(1);
                } else if (operands.get(0) instanceof RexLiteral && isField(operands.get(1))) {
                    literal = (RexLiteral) operands.get(0);
                } else {
                    return false;
                }

                return SUPPORTED_TYPES.contains(literal.getType().getSqlTypeName());
            }

            if (call.op.kind.belongsTo(SUPPORTED_LOGIC)) {
                for (RexNode operand : call.getOperands()) {
                    if (!isSupported(operand)) {
                        return false;
                    }
                }

                return true;
            }
        }

        return false;
    }

    public static Expression representAsExpression(Filter filter) {
        if (filter == null) {
            return Expressions.constant(null);
        }

        return representAsExpression(filter.getCondition(), filter);
    }

    private static Expression representAsExpression(RexNode node, Filter filter) {
        if (isField(node)) {
            RexInputRef field = resolveField(node);
            // TODO: make sure it's a boolean field ref
            // TODO: proper field name resolution

            String fieldName = filter.getRowType().getFieldList().get(field.getIndex()).getName();

            return Expressions.call(EQUAL, Expressions.constant(fieldName), Expressions.constant(true));
        }

        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;

            if (call.op.kind.belongsTo(SUPPORTED_COMPARISONS)) {
                List<RexNode> operands = call.getOperands();
                RexInputRef field;
                RexLiteral literal;
                if (isField(operands.get(0)) && operands.get(1) instanceof RexLiteral) {
                    field = resolveField(operands.get(0));
                    literal = (RexLiteral) operands.get(1);
                } else if (operands.get(0) instanceof RexLiteral && isField(operands.get(1))) {
                    field = resolveField(operands.get(1));
                    literal = (RexLiteral) operands.get(0);
                } else {
                    throw new IllegalStateException();
                }

                // TODO: proper field name resolution
                String fieldName = filter.getRowType().getFieldList().get(field.getIndex()).getName();
                Comparable literalValue = literal.getValue();

                // TODO: Calcite represents decimal literals as BigDecimal, but fails to generated the proper code
                if (literalValue instanceof BigDecimal) {
                    literalValue = literalValue.toString();
                }

                switch (call.op.kind) {
                    case EQUALS:
                        return Expressions.call(EQUAL, Expressions.constant(fieldName), Expressions.constant(literalValue));
                    case NOT_EQUALS:
                        return Expressions.call(NOT_EQUAL, Expressions.constant(fieldName), Expressions.constant(literalValue));
                    case LESS_THAN:
                        return Expressions.call(LESS_THAN, Expressions.constant(fieldName), Expressions.constant(literalValue));
                    case GREATER_THAN:
                        return Expressions.call(GREATER_THAN, Expressions.constant(fieldName),
                                Expressions.constant(literalValue));
                    case LESS_THAN_OR_EQUAL:
                        return Expressions.call(LESS_EQUAL, Expressions.constant(fieldName), Expressions.constant(literalValue));
                    case GREATER_THAN_OR_EQUAL:
                        return Expressions.call(GREATER_EQUAL, Expressions.constant(fieldName),
                                Expressions.constant(literalValue));
                }
            }

            if (call.op.kind.belongsTo(SUPPORTED_LOGIC)) {
                List<RexNode> operands = call.getOperands();

                switch (call.op.kind) {
                    case AND:
                        Expression[] andExpressions = new Expression[operands.size()];
                        for (int i = 0; i < operands.size(); ++i) {
                            andExpressions[i] = representAsExpression(operands.get(i), filter);
                        }
                        return Expressions.call(AND, andExpressions);
                    case OR:
                        Expression[] orExpressions = new Expression[operands.size()];
                        for (int i = 0; i < operands.size(); ++i) {
                            orExpressions[i] = representAsExpression(operands.get(i), filter);
                        }
                        return Expressions.call(OR, orExpressions);
                    case NOT:
                        assert operands.size() == 1;
                        return Expressions.call(NOT, representAsExpression(operands.get(0), filter));
                }
            }

        }

        throw new IllegalStateException();
    }

    private static boolean isField(RexNode node) {
        // TODO: validate ref is really a table field
        if (node instanceof RexInputRef) {
            return true;
        }

        // TODO: proper cast support
        if (node instanceof RexCall) {
            RexCall rexCall = (RexCall) node;
            if (rexCall.op.kind == SqlKind.CAST) {
                assert rexCall.operands.size() == 1;
                return isField(rexCall.operands.get(0));
            }
        }

        return false;
    }

    private static RexInputRef resolveField(RexNode node) {
        if (node instanceof RexInputRef) {
            return (RexInputRef) node;
        }

        if (node instanceof RexCall) {
            RexCall rexCall = (RexCall) node;
            if (rexCall.op.kind == SqlKind.CAST) {
                assert rexCall.operands.size() == 1;
                return resolveField(rexCall.operands.get(0));
            }
        }

        throw new IllegalStateException();
    }

    private static Method predicate(String name, Class... args) {
        Method method;
        try {
            method = Predicates.class.getMethod(name, args);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
        method.setAccessible(true);
        return method;
    }

    private Filters() {
    }

}
