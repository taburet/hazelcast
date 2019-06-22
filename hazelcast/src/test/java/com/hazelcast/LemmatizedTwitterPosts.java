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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class LemmatizedTwitterPosts {

    private final DataInputStream input;

    private long id;

    private String text;

    private String[] tokens;

    public LemmatizedTwitterPosts(String path) {
        try {
            this.input = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean next() {
        try {
            id = input.readLong();
            text = input.readUTF();
            int tokenCount = (int) input.readLong();
            tokens = new String[tokenCount];
            for (int i = 0; i < tokenCount; ++i) {
                tokens[i] = input.readUTF();
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public long id() {
        return id;
    }

    public String text() {
        return text;
    }

    public String[] tokens() {
        return tokens;
    }

}
