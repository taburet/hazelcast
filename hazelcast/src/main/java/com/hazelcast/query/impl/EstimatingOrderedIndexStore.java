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

import com.hazelcast.nio.serialization.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static java.util.Collections.emptySet;

/**
 * Store indexes rankly.
 */
public class EstimatingOrderedIndexStore extends BaseIndexStore {

    private final BPlusTree<Comparable, Data, QueryableEntry> entries = new BPlusTree<Comparable, Data, QueryableEntry>();
    private final Map<Data, QueryableEntry> nullEntries = new HashMap<Data, QueryableEntry>();

    public EstimatingOrderedIndexStore(IndexCopyBehavior copyOn) {
        super(IndexCopyBehavior.NEVER);
    }

    @Override
    Comparable canonicalizeScalarForStorage(Comparable value) {
        return value;
    }

    @Override
    Object insertInternal(Comparable value, QueryableEntry record) {
        if (value == NULL) {
            return nullEntries.put(record.getKeyData(), record);
        } else {
            return entries.put(value, record.getKeyData(), record);
        }
    }

    @Override
    Object removeInternal(Comparable value, Data recordKey) {
        if (value == NULL) {
            return nullEntries.remove(recordKey);
        } else {
            return entries.remove(value, recordKey);
        }
    }

    @Override
    public Comparable canonicalizeQueryArgumentScalar(Comparable value) {
        return Comparables.canonicalizeForHashLookup(value);
    }

    @Override
    public void clear() {
        takeWriteLock();
        try {
            entries.clear();
            nullEntries.clear();
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        takeReadLock();
        try {
            if (value == NULL) {
                return toSingleResultSet(new HashMap<Data, QueryableEntry>(nullEntries));
            } else {
                return toSingleResultSet(entries.query(value).toMap());
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public long estimateCardinality(Comparable value) {
        takeReadLock();
        try {
            if (value == NULL) {
                return nullEntries.size();
            } else {
                return entries.estimateCardinality(value);
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        takeReadLock();
        try {
            MultiResultSet results = createMultiResultSet();
            for (Comparable value : values) {
                Map<Data, QueryableEntry> records;
                if (value == NULL) {
                    records = new HashMap<Data, QueryableEntry>(nullEntries);
                } else {
                    records = entries.query(value).toMap();
                }
                if (!records.isEmpty()) {
                    copyToMultiResultSet(results, records);
                }
            }
            return results;
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public long estimateCardinality(Set<Comparable> values) {
        long estimate = 0;

        takeReadLock();
        try {
            for (Comparable value : values) {
                if (value == NULL) {
                    estimate += nullEntries.size();
                } else {
                    estimate += entries.estimateCardinality(value);
                }
            }
            return estimate;
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        takeReadLock();
        try {
            Map<Data, QueryableEntry> result;
            switch (comparison) {
                case LESS:
                    result = entries.query(null, false, value, false).toMap();
                    break;
                case LESS_OR_EQUAL:
                    result = entries.query(null, false, value, true).toMap();
                    break;
                case GREATER:
                    result = entries.query(value, false, null, false).toMap();
                    break;
                case GREATER_OR_EQUAL:
                    result = entries.query(value, true, null, false).toMap();
                    break;
                case NOT_EQUAL:
                    if (value == NULL) {
                        result = entries.query().toMap();
                        break;
                    } else {
                        MultiResultSet results = createMultiResultSet();
                        copyToMultiResultSet(results, entries.query(null, false, value, false).toMap());
                        copyToMultiResultSet(results, entries.query(value, false, null, false).toMap());
                        return results;
                    }
                default:
                    throw new IllegalArgumentException("Unrecognized comparison: " + comparison);
            }
            return toSingleResultSet(result);
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public long estimateCardinality(Comparison comparison, Comparable value) {
        takeReadLock();
        try {
            switch (comparison) {
                case LESS:
                    return entries.estimateCardinality(null, false, value, false);
                case LESS_OR_EQUAL:
                    return entries.estimateCardinality(null, false, value, true);
                case GREATER:
                    return entries.estimateCardinality(value, false, null, false);
                case GREATER_OR_EQUAL:
                    return entries.estimateCardinality(value, true, null, false);
                case NOT_EQUAL:
                    if (value == NULL) {
                        return entries.estimateCardinality();
                    } else {
                        return entries.estimateCardinality() - entries.estimateCardinality(value);
                    }
                default:
                    throw new IllegalArgumentException("Unrecognized comparison: " + comparison);
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        takeReadLock();
        try {
            int order = Comparables.compare(from, to);
            if (order == 0) {
                if (!fromInclusive || !toInclusive) {
                    return emptySet();
                }
                return toSingleResultSet(entries.query(from).toMap());
            } else if (order > 0) {
                return emptySet();
            }

            return toSingleResultSet(entries.query(from, fromInclusive, to, toInclusive).toMap());
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public long estimateCardinality(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        takeReadLock();
        try {
            int order = Comparables.compare(from, to);
            if (order == 0) {
                if (!fromInclusive || !toInclusive) {
                    return 0;
                }
                return entries.estimateCardinality(from);
            } else if (order > 0) {
                return 0;
            }

            return entries.estimateCardinality(from, fromInclusive, to, toInclusive);
        } finally {
            releaseReadLock();
        }
    }

}
