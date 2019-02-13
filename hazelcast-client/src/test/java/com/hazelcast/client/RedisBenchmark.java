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

package com.hazelcast.client;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 0)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class RedisBenchmark {

    RedissonClient client;
    RScoredSortedSet sortedSet;

    Jedis jedis;

    @Setup
    public void setup() {
        client = Redisson.create();
        sortedSet = client.getScoredSortedSet("sortedSet");
        sortedSet.clear();

        jedis = new Jedis("127.0.0.1");

        for (int i = 0; i < 10000; ++i) {
            sortedSet.add(i, i);
            jedis.zadd("sortedSet2", i, String.valueOf(i));
        }
    }

    @TearDown
    public void tearDown() {
        client.shutdown();
    }

    @Threads(1)
    @Benchmark
    public void testGet() {
        sortedSet.entryRange(1, false, 9999, false);
//        jedis.zrange("sortedSet2", 1, 1);
    }

}
