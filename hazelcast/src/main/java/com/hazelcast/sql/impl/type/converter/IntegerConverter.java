/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;

/**
 * Converter for {@link java.lang.Integer} type.
 */
public final class IntegerConverter extends Converter {

    public static final IntegerConverter INSTANCE = new IntegerConverter();

    private IntegerConverter() {
        super(ID_INTEGER, QueryDataTypeFamily.INT);
    }

    @Override
    public Class<?> getValueClass() {
        return Integer.class;
    }

    @Override
    public byte asTinyint(Object val) {
        int casted = cast(val);
        byte converted = (byte) casted;

        if (converted != casted) {
            throw numericOverflow(QueryDataTypeFamily.TINYINT, val);
        }

        return converted;
    }

    @Override
    public short asSmallint(Object val) {
        int casted = cast(val);
        short converted = (short) casted;

        if (converted != casted) {
            throw numericOverflow(QueryDataTypeFamily.SMALLINT, val);
        }

        return converted;
    }

    @Override
    public int asInt(Object val) {
        return cast(val);
    }

    @Override
    public long asBigint(Object val) {
        return cast(val);
    }

    @Override
    public BigDecimal asDecimal(Object val) {
        return new BigDecimal(cast(val), DECIMAL_MATH_CONTEXT);
    }

    @Override
    public float asReal(Object val) {
        return cast(val);
    }

    @Override
    public double asDouble(Object val) {
        return cast(val);
    }

    @Override
    public String asVarchar(Object val) {
        return Integer.toString(cast(val));
    }

    @Override
    public Object convertToSelf(Converter valConverter, Object val) {
        return valConverter.asInt(val);
    }

    private int cast(Object val) {
        return (int) val;
    }

}
