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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SqlEnumerableEntrySet extends AbstractEnumerable<Object[]> {

    private final Field[] fields;
    private final Set<Map.Entry<Object, Object>> entrySet;

    public SqlEnumerableEntrySet(Field[] fields, Set<Map.Entry<Object, Object>> entrySet) {
        this.fields = fields;
        this.entrySet = entrySet;
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        return new EnumeratorImpl();
    }

    private class EnumeratorImpl implements Enumerator<Object[]> {

        private Iterator<Map.Entry<Object, Object>> iterator = entrySet.iterator();

        private Object[] current;

        @Override
        public Object[] current() {
            return current;
        }

        @Override
        public boolean moveNext() {
            if (!iterator.hasNext()) {
                return false;
            }

            Object value = iterator.next().getValue();

            current = new Object[fields.length];
            for (int i = 0; i < fields.length; ++i) {
                try {
                    current[i] = fields[i].get(value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            return true;
        }

        @Override
        public void reset() {
            iterator = entrySet.iterator();
        }

        @Override
        public void close() {
        }

    }

}
