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

package com.hazelcast;

import com.hazelcast.config.BitmapIndexOptions;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class SizeOfTest extends HazelcastTestSupport {

    private final Random random = new Random(42);

    private IMap<Integer, Integer> map;

    @Before
    public void before() {
        HazelcastInstance instance = createHazelcastInstance();
        map = instance.getMap("map");

        IndexConfig indexConfig = new IndexConfig(IndexType.BITMAP, "this");
        indexConfig.getBitmapIndexOptions().setUniqueKeyTransformation(BitmapIndexOptions.UniqueKeyTransformation.RAW);
        map.addIndex(indexConfig);

//        map.addIndex(IndexType.SORTED, "this");
//
//        map.addIndex(IndexType.HASH, "this");
    }

    @Test
    public void test() throws InterruptedException {
        for (int i = 0; i < 1_000_000; ++i) {
            map.put(i, i);
        }

        System.out.println(map.getLocalMapStats().getIndexStats());

        Thread.sleep(10000000000L);
        System.out.println(map.size());
    }

}
