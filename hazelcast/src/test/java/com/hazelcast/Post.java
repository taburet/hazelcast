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

package com.hazelcast;

import com.hazelcast.query.impl.MinHash;

import java.io.Serializable;

public class Post implements Serializable {

    private static final int MIN_HASH_WIDTH = 10;
    public static final MinHash MIN_HASH = new MinHash(MIN_HASH_WIDTH);

    private static final int BANDS = 5;
    private static final int BAND_WIDTH = MIN_HASH_WIDTH / BANDS;

    public final long id;

    public final String text;

    public final String[] tokens;

    public Post(long id, String text, String[] tokens) {
        this.id = id;
        this.text = text;
        this.tokens = tokens;
    }

    public long[] getSignature() {
        return MIN_HASH.signature(tokens);
    }

    public int[] getIntSignature() {
        return MIN_HASH.intSignature(tokens);
    }

    public long[] getBands() {
        return MIN_HASH.bands(tokens);
    }

    @Override
    public String toString() {
        return id + ": " + text;
    }

}
