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

import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.Assert.assertEquals;

public class BPlusTreeTest {

    @Test
    public void randomizedUniqueTest() {
        long seed = System.nanoTime();
        System.out.println("Seed: " + seed + "L");
        Random random = new Random(seed);

        NavigableMap<Integer, Double> expected = new ConcurrentSkipListMap<Integer, Double>(); // new TreeMap<>();
        BPlusTree<Integer, Long, Double> actual = new BPlusTree<Integer, Long, Double>();

        for (int i = 0; i < 10000; ++i) {
            int key = random.nextInt(1000);

            if (random.nextInt(10) > 1) {
                expected.put(key, (double) key);
                actual.put(key, (long) key, (double) key);
            } else {
                expected.remove(key);
                actual.remove(key, (long) key);
            }

            assertEqualIncludingOrder(expected, actual);
        }
    }

    @Test
    public void queryTest() {
        long seed = System.nanoTime();
        System.out.println("Seed: " + seed + "L");
        Random random = new Random(seed);

        NavigableMap<Integer, Double> expected = new ConcurrentSkipListMap<Integer, Double>(); // new TreeMap<>();
        BPlusTree<Integer, Long, Double> actual = new BPlusTree<Integer, Long, Double>();

        for (int i = 0; i < 10000; ++i) {
            int key = random.nextInt(1000);

            if (random.nextInt(10) > 1) {
                expected.put(key, (double) key);
                actual.put(key, (long) key, (double) key);
            } else {
                expected.remove(key);
                actual.remove(key, (long) key);
            }

            assertEqualIncludingOrder(expected, actual);
        }

        for (int i = 0; i < 100; ++i) {
            int key = random.nextInt(1000);
            Double value = expected.get(key);

            assertEqualIncludingOrder(value == null ? Collections.emptyMap() : Collections.singletonMap(key, value),
                    actual.query(key));
        }

        for (int i = 0; i < 1000; ++i) {
            int from = random.nextInt(500);
            boolean fromInclusive = random.nextBoolean();
            int to = 500 + random.nextInt(500);
            boolean toInclusive = random.nextBoolean();

            assertEqualIncludingOrder(expected.subMap(from, fromInclusive, to, toInclusive),
                    actual.query(from, fromInclusive, to, toInclusive));
        }

        for (int i = 0; i < 1000; ++i) {
            int key = random.nextInt(1000);
            boolean inclusive = random.nextBoolean();

            assertEqualIncludingOrder(expected.tailMap(key, inclusive), actual.query(key, inclusive, null, false));
            assertEqualIncludingOrder(expected.headMap(key, inclusive), actual.query(null, false, key, inclusive));
        }
    }

    private static void assertEqualIncludingOrder(Map expected, BPlusTree actual) {
        assertEqualIncludingOrder(expected, actual.query());
    }

    @SuppressWarnings("unchecked")
    private static void assertEqualIncludingOrder(Map expected, BPlusTree.Cursor actual) {
        Iterator<Map.Entry> iterator = expected.entrySet().iterator();

        while (true) {
            assertEquals(iterator.hasNext(), actual.next());
            if (!iterator.hasNext()) {
                break;
            }

            assertEquals(iterator.next().getValue(), actual.value());
        }
    }

}
