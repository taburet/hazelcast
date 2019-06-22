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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LemmatizingConverter {

    public static void main(String[] args) throws IOException {
        DataOutputStream output =
                new DataOutputStream(new BufferedOutputStream(new FileOutputStream("/Users/sergey/Downloads/posts.bin")));

        TwitterPosts twitterPosts = new TwitterPosts("/Users/sergey/Downloads/training.1600000.processed.noemoticon.csv");
        LemmatizingTokenizer tokenizer = new LemmatizingTokenizer();

        int counter = 0;
        List<String> tokens = new ArrayList<>();
        while (twitterPosts.next()) {
            if (++counter % 10000 == 0) {
                System.out.println(twitterPosts.id() + ": " + twitterPosts.text());
            }

//            if (counter == 50000) {
//                break;
//            }

            tokenizer.setText(twitterPosts.text());
            tokens.clear();
            String token;
            while ((token = tokenizer.next()) != null) {
                tokens.add(token);
            }

            output.writeLong(twitterPosts.id());
            output.writeUTF(twitterPosts.text());
            output.writeLong(tokens.size());
            for (String t : tokens) {
                output.writeUTF(t);
            }
        }

        output.close();
    }

}
