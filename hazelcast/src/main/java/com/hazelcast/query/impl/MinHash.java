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

import java.util.Iterator;
import java.util.Random;

import static java.lang.Integer.rotateRight;

public class MinHash {

    private final int width;
    private final int[] seeds;

    public MinHash(int width) {
        this.width = width;
        Random random = new Random();

        seeds = new int[width];
        for (int i = 0; i < width; ++i) {
            seeds[i] = random.nextInt();
        }
    }

    public long[] signature(Iterator values) {
        long[] signature = new long[width];
//        for (int i = 0; i < width; ++i) {
//            signature[i] = 0xFFFFFFFFL;
//        }

        while (values.hasNext()) {
            Object value = values.next();
            int hashCode = value.hashCode();

            for (int i = 0; i < width; ++i) {
                long componentHashCode = (rotateRight(hashCode, seeds[i] & 0b11111) ^ seeds[i]) & 0xFFFFFFFFL;
                if (componentHashCode > signature[i]) {
                    signature[i] = componentHashCode;
                }
            }
        }

        for (int i = 0; i < width; ++i) {
            signature[i] |= (long) i << 32;
        }

        return signature;
    }

    public long[] signature(Object[] values) {
        long[] signature = new long[width];
//        for (int i = 0; i < width; ++i) {
//            signature[i] = 0xFFFFFFFFL;
//        }

        for (Object value : values) {
            int hashCode = value.hashCode();

            for (int i = 0; i < width; ++i) {
                long componentHashCode = (rotateRight(hashCode, seeds[i] & 0b11111) ^ seeds[i]) & 0xFFFF_FFFFL;
                if (componentHashCode > signature[i]) {
                    signature[i] = componentHashCode;
                }
            }
        }

        for (int i = 0; i < width; ++i) {
            signature[i] |= (long) i << 32;
        }

        return signature;
    }

    public int[] intSignature(Object[] values) {
        int[] signature = new int[width];
        for (int i = 0; i < width; ++i) {
            signature[i] = Integer.MAX_VALUE;
        }

        for (Object value : values) {
            int hashCode = value.hashCode();

            for (int i = 0; i < width; ++i) {
                int componentHashCode = rotateRight(hashCode, seeds[i] & 0b11111) ^ seeds[i];
                if (componentHashCode < signature[i]) {
                    signature[i] = componentHashCode;
                }
            }
        }

        return signature;
    }

    public long[] bands(Object[] values) {
        long[] signature = signature(values);

        long[] bands = new long[5];
        for (int i = 0; i < 5; ++i) {
            for (int j = 0; j < 2; ++j) {
                bands[i] ^= signature[i * 2 + j];
            }

            bands[i] |= (long) i << 32;
        }

        return bands;
    }

}
