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

package com.hazelcast.query.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public final class BPlusTree<K extends Comparable, I, V> {

    // TODO: don't hit leaf nodes while estimating cardinality, there is enough data on higher levels
    // TODO: link internal nodes to simplify traversal while estimating
    // TODO: move cursor concept to Index to avoid copying
    // TODO: replace arraycopy with a single population counting loop while splitting
    // TODO: represent OneRecord as a map also
    // TODO: move cardinality estimation to Cursor and optimize to avoid double lookups

    private static final int FAN_OUT = 128;
    private static final int DATA_SIZE = FAN_OUT * 2;
    private static final int SPLIT_SIZE = FAN_OUT / 2;
    private static final int SPLIT_DATA_SIZE = SPLIT_SIZE * 2;

    private Node root = new Node();

    @SuppressWarnings("unchecked")
    public V put(K key, I id, V value) {
        PutResult result = root.put(key, id, value);

        if (result != null && result.splitOutNode != null) {
            Node right = result.splitOutNode;
            Comparable separator = result.separator;

            Node newRoot = new Node();
            newRoot.internal = true;
            newRoot.size = 1;
            newRoot.population = root.population + right.population;
            newRoot.link = root;
            newRoot.data[0] = separator;
            newRoot.data[1] = right;
            root = newRoot;
        }

        return result == null ? null : (V) result.previousValue;
    }

    @SuppressWarnings("unchecked")
    public Cursor<I, V> query() {
        return new AllRecordsCursor(leftmostLeaf());
    }

    public long estimateCardinality() {
        return root.population;
    }

    @SuppressWarnings("unchecked")
    public Cursor<I, V> query(K key) {
        Node leaf = findLeaf(key);
        int index = leaf.search(key);

        if (index < 0) {
            return EmptyCursor.INSTANCE;
        }

        Object record = leaf.data[(index << 1) + 1];
        if (record instanceof OneRecord) {
            return new OneRecordCursor((OneRecord) record);
        } else {
            return new ManyRecordsCursor((ManyRecords) record);
        }
    }

    public long estimateCardinality(K key) {
        Node leaf = findLeaf(key);
        int index = leaf.search(key);

        if (index < 0) {
            return 0;
        }

        Object record = leaf.data[(index << 1) + 1];
        if (record instanceof OneRecord) {
            return 1;
        } else {
            return ((ManyRecords) record).size();
        }
    }

    @SuppressWarnings("unchecked")
    public Cursor<I, V> query(K from, boolean fromInclusive, K to, boolean toInclusive) {
        // TODO: support from > to (?)

        Node fromLeaf;
        int fromIndex;
        if (from == null) {
            fromLeaf = leftmostLeaf();
            fromIndex = 0;
        } else {
            fromLeaf = findLeaf(from);
            fromIndex = fromLeaf.search(from);
            if (fromIndex < 0) {
                fromIndex = -(fromIndex + 1);
            } else if (!fromInclusive) {
                ++fromIndex;
            }
        }

        Node toLeaf;
        int toIndex;
        if (to == null) {
            toLeaf = null;
            // that may be any value
            toIndex = 0;
        } else {
            toLeaf = findLeaf(to);
            toIndex = toLeaf.search(to);
            if (toIndex < 0) {
                toIndex = -(toIndex + 1) - 1;
            } else if (!toInclusive) {
                --toIndex;
            }
        }

        return new RangeCursor(fromLeaf, fromIndex, toLeaf, toIndex);
    }

    public long estimateCardinality(K from, boolean fromInclusive, K to, boolean toInclusive) {
        // TODO: support from > to (?)

        Node fromLeaf;
        int fromIndex;
        if (from == null) {
            fromLeaf = leftmostLeaf();
            fromIndex = 0;
        } else {
            fromLeaf = findLeaf(from);
            fromIndex = fromLeaf.search(from);
            if (fromIndex < 0) {
                fromIndex = -(fromIndex + 1);
            } else if (!fromInclusive) {
                ++fromIndex;
            }
        }

        Node toLeaf;
        int toIndex;
        if (to == null) {
            toLeaf = null;
            toIndex = 0;
        } else {
            toLeaf = findLeaf(to);
            toIndex = toLeaf.search(to);
            if (toIndex < 0) {
                toIndex = -(toIndex + 1) - 1;
            } else if (!toInclusive) {
                --toIndex;
            }
        }

        long estimate;

        if (fromIndex == 0) {
            estimate = fromLeaf.population;
        } else {
            estimate = 0;
            for (int i = fromIndex; i < fromLeaf.size; ++i) {
                Object record = fromLeaf.data[(i << 1) + 1];
                if (record instanceof OneRecord) {
                    ++estimate;
                } else {
                    estimate += ((ManyRecords) record).size();
                }
            }
        }

        if (toLeaf != fromLeaf) {
            Node leaf = fromLeaf.link;
            while (leaf != toLeaf) {
                estimate += leaf.population;
                leaf = leaf.link;
            }
        }

        if (toLeaf != null) {
            for (int i = 0; i <= Math.min(toIndex, fromLeaf.size - 1); ++i) {
                Object record = toLeaf.data[(i << 1) + 1];
                if (record instanceof OneRecord) {
                    ++estimate;
                } else {
                    estimate += ((ManyRecords) record).size();
                }
            }
        }

        return estimate;
    }

    @SuppressWarnings("unchecked")
    public V remove(K key, I id) {
        return (V) root.remove(key, id);
    }

    public void clear() {
        root.clear();
    }

    private Node findLeaf(K key) {
        Node node = root;
        while (node.internal) {
            int index = node.search(key);
            if (index < 0) {
                index = -(index + 1);
                if (index == 0) {
                    node = node.link;
                } else {
                    node = (Node) node.data[((index - 1) << 1) + 1];
                }
            } else {
                node = (Node) node.data[(index << 1) + 1];
            }
        }
        return node;
    }

    private Node leftmostLeaf() {
        Node node = root;
        while (node.internal) {
            node = node.link;
        }
        return node;
    }

    public interface Cursor<I, V> {

        boolean next();

        I id();

        V value();

        Map<I, V> toMap();

    }

    private static class EmptyCursor implements Cursor {

        static final Cursor INSTANCE = new EmptyCursor();

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public Object id() {
            throw new NoSuchElementException();
        }

        @Override
        public Object value() {
            throw new NoSuchElementException();
        }

        @Override
        public Map toMap() {
            return Collections.emptyMap();
        }

    }

    private static class OneRecordCursor implements Cursor {

        private final OneRecord record;
        private boolean consumed;

        OneRecordCursor(OneRecord record) {
            this.record = record;
        }

        @Override
        public boolean next() {
            if (consumed) {
                return false;
            } else {
                consumed = true;
                return true;
            }
        }

        @Override
        public Object id() {
            return record.id;
        }

        @Override
        public Object value() {
            return record.value;
        }

        @Override
        public Map toMap() {
            next();
            return Collections.singletonMap(id(), value());
        }

    }

    private static class ManyRecordsCursor implements Cursor {

        private final BPlusTree.ManyRecords records;
        private final Iterator<Map.Entry> iterator;

        private Map.Entry current;

        @SuppressWarnings("unchecked")
        ManyRecordsCursor(ManyRecords records) {
            this.records = records;
            this.iterator = records.entrySet().iterator();
        }

        @Override
        public boolean next() {
            if (iterator.hasNext()) {
                current = iterator.next();
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Object id() {
            return current.getKey();
        }

        @Override
        public Object value() {
            return current.getValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map toMap() {
            return new HashMap(records);
        }

    }

    private static class AllRecordsCursor implements Cursor {

        private Node leaf;
        private int index;

        private Iterator<Map.Entry> iterator;
        private Map.Entry current;

        AllRecordsCursor(Node leaf) {
            this.leaf = leaf;
            this.index = 0;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean next() {
            if (iterator != null) {
                if (iterator.hasNext()) {
                    current = iterator.next();
                    return true;
                } else {
                    iterator = null;
                }
            }

            if (index == leaf.size) {
                while (true) {
                    if (leaf.link == null) {
                        return false;
                    }

                    leaf = leaf.link;
                    if (leaf.size > 0) {
                        index = 0;
                        break;
                    }
                }
            }

            Object record = leaf.data[(index << 1) + 1];
            if (record instanceof OneRecord) {
                current = (Map.Entry) record;
            } else {
                iterator = ((ManyRecords) record).entrySet().iterator();
                current = iterator.next();
            }

            ++index;
            return true;
        }

        @Override
        public Object id() {
            return current.getKey();
        }

        @Override
        public Object value() {
            return current.getValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map toMap() {
            Map map = new HashMap();
            while (next()) {
                map.put(id(), value());
            }
            return map;
        }

    }

    private static class RangeCursor implements Cursor {

        private final Node toLeaf;
        private final int toIndex;

        private Node leaf;
        private int index;

        private Iterator<Map.Entry> iterator;
        private Map.Entry current;

        RangeCursor(Node fromLeaf, int fromIndex, Node toLeaf, int toIndex) {
            this.leaf = fromLeaf;
            this.index = fromIndex;
            this.toLeaf = toLeaf;
            this.toIndex = toIndex;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean next() {
            if (iterator != null) {
                if (iterator.hasNext()) {
                    current = iterator.next();
                    return true;
                } else {
                    iterator = null;
                }
            }

            if (leaf == toLeaf && index > toIndex) {
                return false;
            }

            if (index == leaf.size) {
                while (true) {
                    if (leaf.link == null) {
                        return false;
                    }

                    leaf = leaf.link;
                    if (leaf == toLeaf && toIndex == -1) {
                        // that may happen if we are excluding the first key of the to node
                        return false;
                    }

                    if (leaf.size > 0) {
                        index = 0;
                        break;
                    }
                }
            }

            Object record = leaf.data[(index << 1) + 1];
            if (record instanceof OneRecord) {
                current = (Map.Entry) record;
            } else {
                iterator = ((ManyRecords) record).entrySet().iterator();
                current = iterator.next();
            }

            ++index;
            return true;
        }

        @Override
        public Object id() {
            return current.getKey();
        }

        @Override
        public Object value() {
            return current.getValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map toMap() {
            Map map = new HashMap();
            while (next()) {
                map.put(id(), value());
            }
            return map;
        }

    }

    private static class PutResult {

        Node splitOutNode;

        Comparable separator;

        Object previousValue;

    }

    private static class Node {

        boolean internal;

        int size;

        long population;

        // left child for internal nodes, right sibling for leafs
        Node link;

        final Object[] data = new Object[DATA_SIZE];

        int search(Comparable key) {
            int low = 0;
            int high = size - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                Comparable storedKey = (Comparable) data[mid << 1];
                int order = Comparables.compare(storedKey, key);

                if (order < 0) {
                    low = mid + 1;
                } else if (order > 0) {
                    high = mid - 1;
                } else {
                    // found
                    return mid;
                }
            }

            // not found
            return -(low + 1);
        }

        void insert(int index, Comparable key, Object value) {
            System.arraycopy(data, index, data, index + 2, (size << 1) - index);
            data[index] = key;
            data[index + 1] = value;
            ++size;
        }

        void delete(int index) {
            --size;
            int beyondIndex = size << 1;
            System.arraycopy(data, index + 2, data, index, beyondIndex - index);
            data[beyondIndex] = null;
            data[beyondIndex + 1] = null;
        }

        @SuppressWarnings("unchecked")
        PutResult update(int index, Object id, Object value) {
            int recordIndex = (index << 1) + 1;
            Object record = data[recordIndex];

            if (record instanceof OneRecord) {
                OneRecord oneRecord = (OneRecord) record;
                if (oneRecord.id.equals(id)) {
                    PutResult result = new PutResult();
                    result.previousValue = oneRecord.value;
                    oneRecord.value = value;
                    return result;
                } else {
                    ManyRecords manyRecords = new ManyRecords();
                    manyRecords.put(oneRecord.id, oneRecord.value);
                    manyRecords.put(id, value);
                    data[recordIndex] = manyRecords;
                    ++population;
                    return null;
                }
            } else {
                ManyRecords manyRecords = (ManyRecords) record;
                Object previousValue = manyRecords.put(id, value);
                if (previousValue != null) {
                    PutResult result = new PutResult();
                    result.previousValue = previousValue;
                    return result;
                } else {
                    ++population;
                    return null;
                }
            }
        }

        PutResult put(Comparable key, Object id, Object value) {
            int index = search(key);

            if (internal) {
                Node node = follow(index);

                PutResult result = node.put(key, id, value);
                if (result != null && result.splitOutNode != null) {
                    Node rightChild = result.splitOutNode;
                    Comparable separator = result.separator;

                    int separatorIndex = search(separator);
                    assert separatorIndex < 0;
                    separatorIndex = -(separatorIndex + 1) << 1;

                    if (size < FAN_OUT) {
                        insert(separatorIndex, separator, rightChild);
                        if (result.previousValue == null) {
                            ++population;
                        }
                        result.splitOutNode = null;
                    } else {
                        splitInternal(separatorIndex, result);
                    }
                } else if (result != null && result.previousValue == null) {
                    ++population;
                }

                return result;
            } else {
                if (index < 0) {
                    index = -(index + 1) << 1;

                    Object record = new OneRecord(id, value);
                    if (size < FAN_OUT) {
                        insert(index, key, record);
                        ++population;
                        return null;
                    } else {
                        return splitLeaf(index, key, record);
                    }
                } else {
                    return update(index, id, value);
                }
            }
        }

        Node follow(int index) {
            if (index < 0) {
                index = -(index + 1);

                if (index == 0) {
                    return link;
                } else {
                    return (Node) data[((index - 1) << 1) + 1];
                }
            } else {
                return (Node) data[(index << 1) + 1];
            }
        }

        PutResult splitLeaf(int index, Comparable key, Object value) {
            Node right = new Node();

            this.size = SPLIT_SIZE;
            right.size = SPLIT_SIZE;

            Node existingRight = this.link;
            this.link = right;
            right.link = existingRight;

            System.arraycopy(this.data, SPLIT_DATA_SIZE, right.data, 0, SPLIT_DATA_SIZE);
            Arrays.fill(this.data, SPLIT_DATA_SIZE, DATA_SIZE, null);

            if (index < SPLIT_DATA_SIZE) {
                this.insert(index, key, value);
            } else {
                right.insert(index - SPLIT_DATA_SIZE, key, value);
            }

            this.population = this.computeLeafPopulation();
            right.population = right.computeLeafPopulation();

            PutResult result = new PutResult();
            result.splitOutNode = right;
            result.separator = (Comparable) right.data[0];

            return result;
        }

        void splitInternal(int separatorIndex, PutResult result) {
            Node right = new Node();
            right.internal = true;

            if (separatorIndex < SPLIT_DATA_SIZE) {
                this.size = SPLIT_SIZE - 1;
                right.size = SPLIT_SIZE;

                Comparable rightSeparator = (Comparable) this.data[SPLIT_DATA_SIZE - 2];
                Node rightLink = (Node) this.data[SPLIT_DATA_SIZE - 1];

                System.arraycopy(this.data, SPLIT_DATA_SIZE, right.data, 0, SPLIT_DATA_SIZE);
                this.insert(separatorIndex, result.separator, result.splitOutNode);

                result.separator = rightSeparator;
                right.link = rightLink;
            } else if (separatorIndex > SPLIT_DATA_SIZE) {
                this.size = SPLIT_SIZE;
                right.size = SPLIT_SIZE - 1;

                Comparable rightSeparator = (Comparable) this.data[SPLIT_DATA_SIZE];
                Node rightLink = (Node) this.data[SPLIT_DATA_SIZE + 1];

                System.arraycopy(this.data, SPLIT_DATA_SIZE + 2, right.data, 0, SPLIT_DATA_SIZE - 2);
                right.insert(separatorIndex - SPLIT_DATA_SIZE - 2, result.separator, result.splitOutNode);

                result.separator = rightSeparator;
                right.link = rightLink;
            } else {
                this.size = SPLIT_SIZE;
                right.size = SPLIT_SIZE;

                System.arraycopy(this.data, SPLIT_DATA_SIZE, right.data, 0, SPLIT_DATA_SIZE);
                right.link = result.splitOutNode;
            }

            Arrays.fill(this.data, SPLIT_DATA_SIZE, DATA_SIZE, null);

            this.population = this.computeInternalPopulation();
            right.population = right.computeInternalPopulation();

            result.splitOutNode = right;
        }

        long computeInternalPopulation() {
            long population = 0;

            for (int i = 0; i < size; ++i) {
                population += ((Node) data[(i << 1) + 1]).population;
            }

            population += link.population;

            return population;
        }

        long computeLeafPopulation() {
            long population = 0;

            for (int i = 0; i < size; ++i) {
                Object record = data[(i << 1) + 1];
                population += record instanceof OneRecord ? 1 : ((ManyRecords) record).size();
            }

            return population;
        }

        Object remove(Comparable key, Object id) {
            int index = search(key);

            if (internal) {
                Node node = follow(index);
                Object removed = node.remove(key, id);
                if (removed != null) {
                    --population;
                }
                return removed;
            } else {
                if (index < 0) {
                    return null;
                }
                index <<= 1;

                Object record = data[index + 1];
                if (record instanceof OneRecord) {
                    OneRecord oneRecord = (OneRecord) record;
                    if (oneRecord.id.equals(id)) {
                        delete(index);
                        --population;
                        return oneRecord.value;
                    } else {
                        return null;
                    }
                } else {
                    ManyRecords manyRecords = (ManyRecords) record;
                    Object removed = manyRecords.remove(id);
                    if (removed == null) {
                        return null;
                    }

                    if (manyRecords.isEmpty()) {
                        delete(index);
                    }
                    --population;
                    return removed;
                }
            }
        }

        void clear() {
            int dataSize = size << 1;
            for (int i = 0; i < dataSize; ++i) {
                data[i] = null;
            }
            size = 0;
            link = null;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            if (!internal) {
                builder.append("* ");
            }
            builder.append(size);
            builder.append(" {");
            for (int i = 0; i < size; ++i) {
                builder.append(data[i << 1]);
                builder.append(": ");
                builder.append(data[(i << 1) + 1]);
                if (i != size - 1) {
                    builder.append(", ");
                }
            }
            builder.append("}");
            return builder.toString();
        }

    }

    private static class OneRecord implements Map.Entry {

        final Object id;

        Object value;

        OneRecord(Object id, Object value) {
            this.id = id;
            this.value = value;
        }

        @Override
        public Object getKey() {
            return id;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public Object setValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return String.valueOf(id);
        }

    }

    private static class ManyRecords extends HashMap {

        ManyRecords() {
            super(4, 1.0F);
        }

    }

}
