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
import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.IMap;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class SqlQuery extends AbstractRelNode implements BindableRel {

    private static final Set<SqlKind> SUPPORTED_LOGIC = EnumSet.of(SqlKind.AND, SqlKind.OR, SqlKind.NOT);

    private static final Set<SqlKind> SUPPORTED_COMPARISONS =
            EnumSet.of(SqlKind.EQUALS, SqlKind.NOT_EQUALS, SqlKind.LESS_THAN, SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL,
                    SqlKind.LESS_THAN_OR_EQUAL);

    private static final Set<SqlTypeName> SUPPORTED_TYPES =
            EnumSet.of(SqlTypeName.BOOLEAN, SqlTypeName.INTEGER, SqlTypeName.DOUBLE, SqlTypeName.CHAR, SqlTypeName.VARCHAR,
                    SqlTypeName.DECIMAL);

    private static final Set<SqlKind> SUPPORTED_AGGREGATIONS =
            EnumSet.of(SqlKind.COUNT, SqlKind.MIN, SqlKind.MAX, SqlKind.AVG, SqlKind.SUM);

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private final SqlTable sqlTable;

    private final RelOptTable table;

    private final Filter filter;

    private final Project project;

    private final Aggregate aggregate;

    public SqlQuery(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, SqlTable sqlTable) {
        this(cluster, traitSet, table, sqlTable, null, null, null);
    }

    private SqlQuery(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, SqlTable sqlTable, Filter filter,
                     Project project, Aggregate aggregate) {
        super(cluster, traitSet);
        assert project == null || aggregate == null;
        this.table = table;
        this.sqlTable = sqlTable;
        this.filter = filter;
        this.project = project;
        this.aggregate = aggregate;
    }

    public SqlQuery tryToCombineWith(Filter filter) {
        if (this.filter != null || !isSupported(filter)) {
            return null;
        }

        return new SqlQuery(getCluster(), getTraitSet(), getTable(), sqlTable, filter, project, aggregate);
    }

    public SqlQuery tryToCombineWith(Project project) {
        if (this.project != null || aggregate != null || !isSupported(project)) {
            return null;
        }

        return new SqlQuery(getCluster(), getTraitSet(), getTable(), sqlTable, filter, project, null);
    }

    public SqlQuery tryToCombineWith(Aggregate aggregate) {
        if (this.aggregate != null || project != null || !isSupported(aggregate)) {
            return null;
        }

        return new SqlQuery(getCluster(), getTraitSet(), getTable(), sqlTable, filter, null, aggregate);
    }

    @Override
    public void register(RelOptPlanner planner) {
        super.register(planner);
        planner.addRule(SqlFilterRule.INSTANCE);
        planner.addRule(SqlProjectRule.INSTANCE);
        planner.addRule(SqlAggregateRule.INSTANCE);
    }

    @Override
    protected RelDataType deriveRowType() {
        if (project != null) {
            return project.getRowType();
        } else if (aggregate != null) {
            return aggregate.getRowType();
        } else {
            return table.getRowType();
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("table", table.getQualifiedName());
        if (filter != null) {
            pw.item("filter", filter.getCondition());
        }
        if (project != null) {
            pw.item("projects", project.getProjects());
        }
        if (aggregate != null) {
            pw.item("aggregate", aggregate.getAggCallList());
        }
        return pw;
    }

    @Override
    public RelOptTable getTable() {
        return table;
    }

    @Override
    public Enumerable<Object[]> bind(DataContext dataContext) {
        return performQuery();
    }

    @Override
    public Class<Object[]> getElementType() {
        return Object[].class;
    }

    @Override
    public Node implement(InterpreterImplementor implementor) {
        // This method is used only during the fallback execution for
        // unsupported queries and their parts.

        Sink sink = implementor.compiler.sink(this);
        return () -> {
            Enumerable<Object[]> enumerable = performQuery();
            Enumerator<Object[]> enumerator = enumerable.enumerator();
            while (enumerator.moveNext()) {
                sink.send(Row.of(enumerator.current()));
            }
            sink.end();
        };
    }

    private Enumerable<Object[]> performQuery() {
        IMap map = sqlTable.getMap();

        Predicate predicate = filter == null ? null : convert(filter);

        if (project != null) {
            return performProjection(map, project.getRowType(), predicate);
        } else if (aggregate != null) {
            return performAggregation(map, aggregate, predicate);
        }

        return performProjection(map, table.getRowType(), predicate);
    }

    @SuppressWarnings("unchecked")
    private static Enumerable<Object[]> performProjection(IMap map, RelDataType rowType, Predicate predicate) {
        List<String> fields = rowType.getFieldNames();
        if (fields.size() == 1) {
            Projection projection = Projections.singleAttribute(fields.get(0));
            Collection rows = predicate == null ? map.project(projection) : map.project(projection, predicate);
            return new SqlArrayEnumerableCollection(rows);
        } else {
            Projection projection = Projections.multiAttribute(fields.toArray(EMPTY_STRING_ARRAY));
            Collection<Object[]> rows = predicate == null ? map.project(projection) : map.project(projection, predicate);
            return new SqlEnumerableCollection<>(rows);
        }
    }

    @SuppressWarnings("unchecked")
    private static Enumerable<Object[]> performAggregation(IMap map, Aggregate aggregate, Predicate predicate) {
        Aggregator aggregator = convert(aggregate);
        Object result = predicate == null ? map.aggregate(aggregator) : map.aggregate(aggregator, predicate);
        return result instanceof Collection ? new SqlArrayEnumerableCollection((Collection) result) : Linq4j.singletonEnumerable(
                new Object[]{result});
    }

    private static boolean isSupported(Filter filter) {
        return isSupported(filter.getCondition());
    }

    private static boolean isSupported(Project project) {
        // TODO: proper fields detection
        for (RexNode node : project.getProjects()) {
            if (!(node instanceof RexInputRef)) {
                return false;
            }
        }

        return true;
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

    private static Predicate convert(Filter filter) {
        return convert(filter.getCondition(), filter);
    }

    private static Predicate convert(RexNode node, Filter filter) {
        if (isField(node)) {
            // TODO: make sure it's a boolean field ref
            // TODO: proper field name resolution

            RexInputRef field = resolveField(node);
            String attribute = filter.getRowType().getFieldList().get(field.getIndex()).getName();
            return Predicates.equal(attribute, true);
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
                String attribute = filter.getRowType().getFieldList().get(field.getIndex()).getName();
                Comparable value = literal.getValue();

                // TODO: Calcite represents decimal literals as BigDecimal, but fails to generated the proper code
                if (value instanceof BigDecimal) {
                    value = value.toString();
                }

                switch (call.op.kind) {
                    case EQUALS:
                        return Predicates.equal(attribute, value);
                    case NOT_EQUALS:
                        return Predicates.notEqual(attribute, value);
                    case LESS_THAN:
                        return Predicates.lessThan(attribute, value);
                    case GREATER_THAN:
                        return Predicates.greaterThan(attribute, value);
                    case LESS_THAN_OR_EQUAL:
                        return Predicates.lessEqual(attribute, value);
                    case GREATER_THAN_OR_EQUAL:
                        return Predicates.greaterEqual(attribute, value);
                }
            }

            if (call.op.kind.belongsTo(SUPPORTED_LOGIC)) {
                List<RexNode> operands = call.getOperands();

                switch (call.op.kind) {
                    case AND:
                        Predicate[] andPredicates = new Predicate[operands.size()];
                        for (int i = 0; i < operands.size(); ++i) {
                            andPredicates[i] = convert(operands.get(i), filter);
                        }
                        return Predicates.and(andPredicates);
                    case OR:
                        Predicate[] orPredicates = new Predicate[operands.size()];
                        for (int i = 0; i < operands.size(); ++i) {
                            orPredicates[i] = convert(operands.get(i), filter);
                        }
                        return Predicates.or(orPredicates);
                    case NOT:
                        assert operands.size() == 1;
                        return Predicates.not(convert(operands.get(0), filter));
                }
            }

        }

        throw new IllegalStateException();
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

    private static boolean isSupported(Aggregate aggregate) {
        // TODO: there is no fields provided by Calcite during optimization (bug?), how to check for fields?

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

    private static boolean isSupported(AggregateCall call) {
        if (call.getArgList().size() > 1 || call.isDistinct()) {
            return false;
        }

        return call.getAggregation().kind.belongsTo(SUPPORTED_AGGREGATIONS);
    }

    private static Aggregator convert(Aggregate aggregate) {
        List<RelDataTypeField> fields = aggregate.getInput().getRowType().getFieldList();

        if (aggregate.getAggCallList().isEmpty() && aggregate.getGroupSets().size() == 1
                && aggregate.getGroupSet().cardinality() == 1 && aggregate.getRowType().getFieldCount() == 1) {
            String attribute = fields.get(aggregate.getGroupSet().nextSetBit(0)).getName();
            return Aggregators.distinct(attribute);
        }

        if (aggregate.getAggCallList().size() == 1 && aggregate.getGroupSets().size() == 1 && aggregate.getGroupSet().isEmpty()
                && aggregate.getRowType().getFieldCount() == 1) {
            // TODO: why aggregate.getGroupSets().size() == 1? is it a bug?

            AggregateCall call = aggregate.getAggCallList().get(0);
            String attribute = call.getArgList().size() == 0 ? null : fields.get(call.getArgList().get(0)).getName();

            switch (call.getAggregation().kind) {
                case COUNT:
                    return attribute == null ? Aggregators.count() : Aggregators.count(attribute);
                case MAX:
                    assert attribute != null;
                    return Aggregators.comparableMax(attribute);
                case MIN:
                    assert attribute != null;
                    return Aggregators.comparableMin(attribute);
                case AVG:
                    assert attribute != null;
                    return Aggregators.numberAvg(attribute);
                case SUM:
                    assert attribute != null;
                    return Aggregators.floatingPointSum(attribute);
            }
        }

        throw new IllegalStateException();
    }

}
