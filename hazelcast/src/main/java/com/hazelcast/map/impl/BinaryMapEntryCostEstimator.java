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

package com.hazelcast.map.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.internal.serialization.Data;

/**
 * Owned entry cost estimator for maps which have {@link InMemoryFormat#BINARY} memory-format.
 */
class BinaryMapEntryCostEstimator
        extends AbstractMapEntryCostEstimator<Record> {

    BinaryMapEntryCostEstimator() {
    }

    @Override
    public long calculateValueCost(Record value) {
        return value.getCost();
    }

}
