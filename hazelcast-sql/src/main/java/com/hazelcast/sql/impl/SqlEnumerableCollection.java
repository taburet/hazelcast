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

import java.util.Collection;
import java.util.Iterator;

public class SqlEnumerableCollection extends AbstractEnumerable<Object> {

    private final Collection<Object> collection;

    public SqlEnumerableCollection(Collection<Object> collection) {
        this.collection = collection;
    }

    @Override
    public Enumerator<Object> enumerator() {
        return new EnumeratorImpl();
    }

    private class EnumeratorImpl implements Enumerator<Object> {

        private Iterator<Object> iterator = collection.iterator();

        private Object current;

        @Override
        public Object current() {
            return current;
        }

        @Override
        public boolean moveNext() {
            if (!iterator.hasNext()) {
                return false;
            }

            current = iterator.next();

            return true;
        }

        @Override
        public void reset() {
            iterator = collection.iterator();
        }

        @Override
        public void close() {
        }

    }

}
