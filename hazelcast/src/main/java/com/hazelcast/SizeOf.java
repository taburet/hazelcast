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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.ConcurrentReferenceHashMap;
import com.hazelcast.query.impl.QueryableEntry;
import org.ehcache.sizeof.filters.CombinationSizeOfFilter;
import org.ehcache.sizeof.impl.UnsafeSizeOf;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.rotateRight;

public final class SizeOf {

    public static final SizeOf INSTANCE = new SizeOf(TimeUnit.SECONDS.toMillis(2));

    private static final org.ehcache.sizeof.SizeOf SHALLOW_SIZE_OF = new UnsafeSizeOf(new CombinationSizeOfFilter(), true, false);
    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    private final long interval;

    private final ConcurrentMap<Object, Record> roots = new ConcurrentReferenceHashMap<>();
    private final Thread worker = new Worker();

    private int counter = 0;

    private SizeOf(long interval) {
        this.interval = interval;
        worker.start();
    }

    public void addRoot(Object root) {
        if (blacklisted(root)) {
            throw new IllegalArgumentException();
        }
        roots.putIfAbsent(root, new Record());
        worker.interrupt();
    }

    private final class Worker extends Thread {

        Worker() {
            setDaemon(true);
            setPriority(MIN_PRIORITY);
        }

        @Override
        public void run() {
            //noinspection InfiniteLoopStatement
            while (true) {
                for (Object root : roots.keySet()) {
                    try {
                        //noinspection BusyWait
                        Thread.sleep(interval);
                    } catch (InterruptedException e) {
                        // do nothing
                    }

                    long start = System.currentTimeMillis();

//                    Record newRecord = sizeOf(root, Collections.newSetFromMap(new IdentityHashMap<>()));

//                    Record record = roots.get(root);
//                    BloomFilter known =
//                            new BloomFilter(record == null ? 1_000 : Math.max(1_000, record.count + record.count / 4), 0.1);
//                    Record newRecord = sizeOf(root, known);

                    Record newRecord = sizeOf(root);

                    DecimalFormat formatter = (DecimalFormat) NumberFormat.getInstance(Locale.US);
                    DecimalFormatSymbols symbols = formatter.getDecimalFormatSymbols();
                    symbols.setGroupingSeparator(' ');
                    formatter.setDecimalFormatSymbols(symbols);

                    System.out.println(root.getClass().getSimpleName() + ": " + formatter.format(newRecord.size) + " bytes, "
                            + formatter.format(newRecord.count) + " objects, " + formatter.format(
                            System.currentTimeMillis() - start) + " ms");
                    roots.computeIfPresent(root, (k, v) -> newRecord);
                }

                if (++counter == 20) {
                    roots.clear();
                    System.out.println("EXIT");
                    break;
                }
            }
        }

        private Record sizeOf(Object root, BloomFilter known) {
            known.add(root);

            Deque<Object> unvisited = new LinkedList<>();
            unvisited.add(root);

            long size = 0;
            int count = 0;

            while (!unvisited.isEmpty()) {
                ++count;

                Object object = unvisited.removeLast();
                Class<?> class_ = object.getClass();

                if (class_.isArray()) {
                    size += SHALLOW_SIZE_OF.sizeOf(object);

                    if (!class_.getComponentType().isPrimitive()) {
                        Object[] array = (Object[]) object;
                        for (Object element : array) {
                            if (!blacklisted(element) && known.add(element)) {
                                unvisited.add(element);
                            }
                        }
                    }
                } else {
                    ClassLayout classLayout = ClassLayout.of(object);

                    size += classLayout.shallowSize();
                    for (MethodHandle reference : classLayout.references()) {
                        try {
                            Object subObject = reference.invoke(object);
                            if (!blacklisted(subObject) && known.add(subObject)) {
                                unvisited.add(subObject);
                            }
                        } catch (Throwable throwable) {
                            //noinspection ThrowablePrintedToSystemOut
                            System.out.println(throwable);
                        }
                    }
                }
            }

            Record record = new Record();
            record.size = size;
            record.count = count;
            return record;
        }

        private Record sizeOf(Object root, Set<Object> known) {
            known.add(root);

            Deque<Object> unvisited = new LinkedList<>();
            unvisited.add(root);

            long size = 0;
            int count = 0;

            while (!unvisited.isEmpty()) {
                ++count;

                Object object = unvisited.removeLast();
                Class<?> class_ = object.getClass();

                if (class_.isArray()) {
                    size += SHALLOW_SIZE_OF.sizeOf(object);

                    if (!class_.getComponentType().isPrimitive()) {
                        Object[] array = (Object[]) object;
                        for (Object element : array) {
                            if (!blacklisted(element) && known.add(element)) {
                                unvisited.add(element);
                            }
                        }
                    }
                } else {
                    ClassLayout classLayout = ClassLayout.of(object);

                    size += classLayout.shallowSize();
                    for (MethodHandle reference : classLayout.references()) {
                        try {
                            Object subObject = reference.invoke(object);
                            if (!blacklisted(subObject) && known.add(subObject)) {
                                unvisited.add(subObject);
                            }
                        } catch (Throwable throwable) {
                            //noinspection ThrowablePrintedToSystemOut
                            System.out.println(throwable);
                        }
                    }
                }
            }

            Record record = new Record();
            record.size = size;
            record.count = count;
            return record;
        }

        private Record sizeOf(Object root) {
            Deque<Object> unvisited = new LinkedList<>();
            unvisited.add(root);

            long size = 0;
            int count = 0;

            while (!unvisited.isEmpty()) {
                ++count;

                Object object = unvisited.removeLast();
                Class<?> class_ = object.getClass();

                if (class_.isArray()) {
                    size += SHALLOW_SIZE_OF.sizeOf(object);

                    if (!class_.getComponentType().isPrimitive()) {
                        Object[] array = (Object[]) object;
                        for (Object element : array) {
                            if (!blacklisted(element)) {
                                unvisited.add(element);
                            }
                        }
                    }
                } else {
                    ClassLayout classLayout = ClassLayout.of(object);

                    size += classLayout.shallowSize();
                    for (MethodHandle reference : classLayout.references()) {
                        try {
                            Object subObject = reference.invoke(object);
                            if (!blacklisted(subObject)) {
                                unvisited.add(subObject);
                            }
                        } catch (Throwable throwable) {
                            //noinspection ThrowablePrintedToSystemOut
                            System.out.println(throwable);
                        }
                    }
                }
            }

            Record record = new Record();
            record.size = size;
            record.count = count;
            return record;
        }

    }

    private static final class ClassLayout {

        private static final Map<Class<?>, ClassLayout> LAYOUTS = new HashMap<>();
        private static final MethodHandle[] EMPTY_REFERENCES = new MethodHandle[0];

        static ClassLayout of(Object object) {
            return LAYOUTS.computeIfAbsent(object.getClass(), k -> new ClassLayout(k, SHALLOW_SIZE_OF.sizeOf(object)));
        }

        private final long shallowSize;

        private final MethodHandle[] references;

        private ClassLayout(Class<?> class_, long shallowSize) {
            this.shallowSize = shallowSize;
            this.references = referencesOf(class_);
        }

        long shallowSize() {
            return shallowSize;
        }

        MethodHandle[] references() {
            return references;
        }

        private static MethodHandle[] referencesOf(Class<?> class_) {
            List<MethodHandle> references = new ArrayList<>();
            referencesOf(class_, references);
            return references.toArray(EMPTY_REFERENCES);
        }

        private static void referencesOf(Class<?> class_, List<MethodHandle> references) {
            if (class_ == null || class_.isInterface()) {
                return;
            }

            for (Field field : class_.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }

                Class<?> type = field.getType();
                if (type.isPrimitive()) {
                    continue;
                }

                if (field.isAnnotationPresent(Unowned.class)) {
                    continue;
                }

                if (field.isSynthetic()) {
                    assert field.getName().startsWith("this$");
                    continue;
                }

                //System.out.println(field);

                field.setAccessible(true);
                try {
                    references.add(LOOKUP.unreflectGetter(field));
                } catch (IllegalAccessException e) {
                    //noinspection ThrowablePrintedToSystemOut
                    System.out.println(e);
                }
            }

            referencesOf(class_.getSuperclass(), references);
        }

    }

    private static boolean blacklisted(Object object) {
        return object == null || object instanceof QueryableEntry || object instanceof InternalSerializationService
                || object instanceof Data;
    }

    private static final class BloomFilter {

        private final long[] bits;
        private final int size;
        private final int[] seeds;

        BloomFilter(int expectedSize, double error) {
            double hashes = Math.ceil(-(Math.log(error) / Math.log(2)));
            double bitsPerElement = hashes / Math.log(2);

            int longs = (int) Math.ceil(expectedSize * bitsPerElement / Long.SIZE);
            this.bits = new long[longs];
            this.size = longs * Long.SIZE;

            int hashCount = (int) hashes;
            this.seeds = new int[hashCount];
            for (int i = 0; i < hashCount; ++i) {
                seeds[i] = ThreadLocalRandom.current().nextInt();
            }
        }

        boolean add(Object element) {
            return add(System.identityHashCode(element));
        }

        boolean add(int element) {
            int counter = 0;
            for (int seed : seeds) {
                int hash = (rotateRight(element, seed & 0b1_1111) ^ seed) & 0x7FFFFFFF;
                int bitIndex = hash % size;
                int longIndex = bitIndex >>> 6;

                long value = bits[longIndex];
                long newValue = value | 1L << bitIndex;
                if (newValue != value) {
                    ++counter;
                    bits[longIndex] = newValue;
                }
            }
            return counter > 0;
        }

    }

    private static final class Record {

        long size;

        int count;

    }

}
