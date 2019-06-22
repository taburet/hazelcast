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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

public class SimpleTokenizer {

    private static final Pattern WHITESPACE = Pattern.compile("[\\s,.:?!\"/\\\\#\\-]+");

    private static final Set<String> STOP_LIST = new HashSet<>();

    static {
        STOP_LIST.add("");
        try {
            Files.lines(Paths.get("/Users/sergey/Downloads/stop_words_english.txt")).forEach(STOP_LIST::add);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Iterator<String> iterator;

    public void setText(String text) {
        this.iterator = Arrays.stream(WHITESPACE.split(text.toLowerCase())).iterator();
    }

    public String next() {
        while (true) {
            if (!iterator.hasNext()) {
                return null;
            }

            String token = iterator.next();
            if (!STOP_LIST.contains(token)) {
                return token;
            }
        }
    }

}
