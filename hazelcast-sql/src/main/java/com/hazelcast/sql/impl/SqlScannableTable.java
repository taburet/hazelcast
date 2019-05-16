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

import com.hazelcast.core.IMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

public class SqlScannableTable implements ScannableTable {

    private final RelDataType rowType;
    private final IMap<Object, Object> map;
    private final Field[] fields;
    private final Statistic statistic = new StatisticImpl();
    private final int keyFieldIndex;

    public SqlScannableTable(Class<?> type, RelDataType rowType, IMap<Object, Object> map) {
        this.rowType = rowType;
        this.map = map;

        fields = new Field[rowType.getFieldCount()];
        for (RelDataTypeField field : rowType.getFieldList()) {
            try {
                Field reflectionField = type.getField(field.getName());
                reflectionField.setAccessible(true);
                fields[field.getIndex()] = reflectionField;
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
        }

        keyFieldIndex = rowType.getField("__key", true, false).getIndex();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType;
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
        throw new IllegalStateException();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new SqlEnumerableEntrySet(fields, map.entrySet());
    }

    private class StatisticImpl implements Statistic {

        @Override
        public Double getRowCount() {
            return null;
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            return columns.get(keyFieldIndex);
        }

        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return Collections.emptyList();
        }

        @Override
        public List<RelCollation> getCollations() {
            return Collections.emptyList();
        }

        @Override
        public RelDistribution getDistribution() {
            return RelDistributionTraitDef.INSTANCE.getDefault();
        }

    }

}
