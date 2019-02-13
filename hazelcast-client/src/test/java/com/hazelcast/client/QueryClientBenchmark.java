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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.SqlPredicate;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1, warmups = 0)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
public class QueryClientBenchmark {

    IMap map;
    Random random;

    @Setup
    public void prepare() {
        random = new Random();

        DataSerializableFactory factory = new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                return new Person();
            }
        };

        Config config = new Config();
        config.getSerializationConfig().addDataSerializableFactory(10, factory);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        MapConfig mapConfig = new MapConfig("persons");
        config.addMapConfig(mapConfig);
        mapConfig.addMapIndexConfig(new MapIndexConfig("age", true));
        mapConfig.addMapIndexConfig(new MapIndexConfig("iq", false));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addDataSerializableFactory(10, factory);
        map = HazelcastClient.newHazelcastClient(clientConfig).getMap("persons");

//        map = hz.getMap("persons");
        for (int k = 0; k < 10000; k++) {
            Person person = new Person(k, 100);
            map.put(k, person);
        }

        System.out.println("jadhjahd");
    }

    @TearDown
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Benchmark
    public void testAllIndices() {
//        int key = random.nextInt();
//        map.put(key, new Person(key, 100));
//        map.keySet(new SqlPredicate("age < 10 and iq=100"));

        map.keySet(new SqlPredicate("age >= 1 and age < 2"));
    }

    @Benchmark
    public void testSuppression() {
        map.keySet(new SqlPredicate("age=10 and %iq=100"));
    }

    public static class Person implements IdentifiedDataSerializable {

        private int iq;
        private int age;

        public Person() {
        }

        public Person(int age, int iq) {
            this.age = age;
            this.iq = iq;
        }

        public int getIq() {
            return iq;
        }

        public int getAge() {
            return age;
        }

        @Override
        public int getFactoryId() {
            return 10;
        }

        @Override
        public int getId() {
            return 10;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(age);
//            out.writeInt(iq);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            age = in.readInt();
//            iq = in.readInt();
        }

        @Override
        public String toString() {
            return "Person{" + "age=" + age + ", iq=" + iq + '}';
        }

    }

}

