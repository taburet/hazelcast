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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Iterator;

public class TwitterPosts {

    // https://www.kaggle.com/kazanova/sentiment140

    private long id;

    private String text;

    private CsvReader reader;

    public TwitterPosts(String path) {
        reader = new CsvReader(path);
    }

    public boolean next() {
        if (!reader.nextRow()) {
            return false;
        }

        reader.skipValue(); // target
        id = Long.parseLong(reader.value()); // id
        reader.skipValue(); // date
        reader.skipValue(); // flag
        reader.skipValue(); // user
        text = reader.value(); // text

        return true;
    }

    public long id() {
        return id;
    }

    public String text() {
        return text;
    }

    private static class CsvReader {

        private final Iterator<String> iterator;

        private final StringBuilder value = new StringBuilder();

        private final StringBuilder escape = new StringBuilder();

        private String row;

        private int position;

        public CsvReader(String path) {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.IGNORE);
            try {
                iterator =
                        new BufferedReader(Channels.newReader(FileChannel.open(Paths.get(path)), decoder, -1)).lines().iterator();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public boolean nextRow() {
            if (!iterator.hasNext()) {
                return false;
            }

            row = iterator.next();
            position = 0;
            return true;
        }

        public String value() {
            value.setLength(0);

            char c = row.charAt(position);
            if (c == '"') {
                while (++position < row.length()) {
                    c = row.charAt(position);
                    if (c == '"') {
                        if (++position < row.length()) {
                            c = row.charAt(position);
                            if (c != ',') {
                                throw new IllegalStateException("unexpected character: " + c);
                            }
                            ++position;
                        }
                        return value.toString();
                    }
                    value.append(c == '&' ? decodeEscape() : c);
                }
                throw new IllegalStateException("unclosed quoted value");
            }

            value.append(c);
            while (++position < row.length()) {
                c = row.charAt(position);
                if (c == ',') {
                    ++position;
                    break;
                }
                value.append(c == '&' ? decodeEscape() : c);
            }
            return value.toString();
        }

        public void skipValue() {
            if (!nextColumn()) {
                throw new IllegalStateException("column expected");
            }
        }

        private boolean nextColumn() {
            if (position >= row.length()) {
                return false;
            }

            if (row.charAt(position) == '"') {
                while (++position < row.length()) {
                    if (row.charAt(position) == '"') {
                        ++position;
                        if (position < row.length()) {
                            if (row.charAt(position) != ',') {
                                throw new IllegalStateException("unexpected character: " + row.charAt(position));
                            }
                            ++position;
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
                throw new IllegalStateException("unclosed quoted value");
            }

            while (++position < row.length()) {
                if (row.charAt(position) == ',') {
                    ++position;
                    return true;
                }
            }
            return false;
        }

        private char decodeEscape() {
            assert row.charAt(position) == '&';

            escape.setLength(0);
            while (++position < row.length()) {
                char c = row.charAt(position);
                if (c == ';') {
                    String escape = this.escape.toString();
                    switch (escape) {
                        case "quot":
                            return '"';
                        case "amp":
                            return '&';
                        case "lt":
                            return '<';
                        case "gt":
                            return '>';
                        default:
                            throw new IllegalStateException("unexpected escape: " + escape);
                    }
                }
                escape.append(c);
            }

            throw new IllegalStateException("unclosed escape");
        }

    }

}
