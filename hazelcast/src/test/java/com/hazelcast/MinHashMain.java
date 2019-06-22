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

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicates;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

public class MinHashMain {

//no index, no NLP
//9.6s
//1.4GB
//
//hash index, no NLP
//0.8s
//2GB
//
//hash index, NLP
//1.1s
//2.1GB
//
//minhash index 15, NLP
//0.8s
//2.8GB
//error: 0%
//
//minhash index 10, NLP
//0.7s
//2.4GB
//error: 0%
//
//minhash index 8, NLP
//0.2s
//2.2GB
//error: 18%
//
//minhash index 5, NLP
//0.1s
//1.9GB
//error: 45%
//
//minhash(10) + banding(2), NLP
//0.001s
//2.4GB
//error: 66%
//
//Last days at school, I think I will cry.
//
//I think i'm going to start crying on the last day of every class in school, just cause ima dork like that. I probably will on
// Tuesday...
//Getting ready for the day! Going to school  but i think its the last day! See yall tonight!
//I feel like crying  Last ever day at my school in a month. Before it gets knocked down and we have to merge with another school
//Can't believe the last day of school was yesterday. i cried
//Today is the last day of school...I was looking back at stuff from this year, and started crying...I hate this! I wanna stay
// in 6th grade
//Graduation Last Night! still 4 days left of school. ugh, and i think we have to run the mile today.
//so tired  last real day of school i think lol
//At school. Last Day. Signing more yearbooks. Ruby Tuesday in a couple hours. Im about to start crying.
//Cried at the last day of school today.. I'll miss all those teachers who got outta school
//Last day of High School  I almost cried

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config.getMapConfig("posts").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        IMap<Long, Post> posts = instance.getMap("posts");
//        posts.addIndex("tokens[any] -> __key?", false);
//        posts.addIndex("tokens[any]", false);

        posts.addIndex("bands[any]", false);

        // https://www.kaggle.com/kazanova/sentiment140
//        TwitterPosts twitterPosts = new TwitterPosts("/Users/sergey/Downloads/training.1600000.processed.noemoticon.csv");
        LemmatizedTwitterPosts twitterPosts = new LemmatizedTwitterPosts("/Users/sergey/Downloads/posts.bin");
        LemmatizingTokenizer tokenizer = new LemmatizingTokenizer();
//        SimpleTokenizer tokenizer = new SimpleTokenizer();

        int counter = 0;
        Set<String> tokens = new HashSet<>();
        while (twitterPosts.next()) {
            if (++counter % 10_000 == 0) {
                System.out.println(twitterPosts.id() + ": " + twitterPosts.text());
            }

//            if (counter == 100_000) {
//                break;
//            }

////            System.out.println(twitterPosts.text());
//            tokenizer.setText(twitterPosts.text());
//            tokens.clear();
//            String token;
//            while ((token = tokenizer.next()) != null) {
////                System.out.print(token + " ");
//                tokens.add(token);
//            }
////            System.out.println();

            tokens.clear();
            Collections.addAll(tokens, twitterPosts.tokens());

            posts.put(twitterPosts.id(), new Post(twitterPosts.id(), twitterPosts.text(), tokens.toArray(EMPTY_STRING_ARRAY)));
        }
        System.out.println("Ingestion finished");
        System.out.println();

//        trying to reduce the number of cigarettes in each day!
//        String query =
//                "@DjAlizay I really don't think people choose to be that way. But I think he chose not to accept my family's "
//                        + "help   He might be dead by now";
//        String query =
//                "is upset that he can't update his Facebook by texting it... and might cry as a result  School today also.
//                Blah!";
//        String query = "I see some data written on a notepad, and saved on my desktop. But I have no clue what it is.";
        String query = "Last days in school, I think I will cry.";
        System.out.println("Searching for: " + query);
        System.out.println();
        tokenizer.setText(query);
        tokens.clear();
        String token;
        while ((token = tokenizer.next()) != null) {
            tokens.add(token);
        }
        long[] signature = Post.MIN_HASH.bands(tokens.toArray(EMPTY_STRING_ARRAY));
        //Post.MIN_HASH.signature(tokens.toArray(EMPTY_STRING_ARRAY));
        Long[] boxedSignature = Arrays.stream(signature).boxed().toArray(Long[]::new);

        System.out.println("True top 10:");
        Collection<Post> trueTop10 = posts.values(
                new PagingPredicate<Long, Post>(Predicates.in("tokens[any]", tokens.toArray(EMPTY_STRING_ARRAY)), (a, b) -> {
                    int intersectionA = computeIntersection(tokens, a.getValue());
                    int intersectionB = computeIntersection(tokens, b.getValue());
                    return intersectionB - intersectionA;
                }, 10));
        trueTop10.forEach(System.out::println);

        long start = 0;
        for (int i = 0; i < 110; ++i) {
            if (i == 10) {
                start = System.currentTimeMillis();
            }

//            Collection<Post> result = posts.values(
//                    new PagingPredicate<Long, Post>(Predicates.in("tokens[any]", tokens.toArray(EMPTY_STRING_ARRAY)), (a, b)
//                    -> {
//                        int intersectionA = computeIntersection(tokens, a.getValue());
//                        int intersectionB = computeIntersection(tokens, b.getValue());
//                        return intersectionB - intersectionA;
//                    }, 10));

            Collection<Post> result =
                    posts.values(new PagingPredicate<Long, Post>(Predicates.in("bands[any]", boxedSignature), (a, b) -> {
                        int intersectionA = computeIntersection(tokens, a.getValue());
                        int intersectionB = computeIntersection(tokens, b.getValue());
                        return intersectionB - intersectionA;
                    }, 10));

            if (i == 0) {
                Set<Long> intersection = trueTop10.stream().map(p -> p.id).collect(Collectors.toSet());
                intersection.retainAll(result.stream().map(p -> p.id).collect(Collectors.toSet()));

                Set<Long> union = trueTop10.stream().map(p -> p.id).collect(Collectors.toSet());
                union.addAll(result.stream().map(p -> p.id).collect(Collectors.toSet()));

                System.out.println("\nEstimated top 10:");
                System.out.println("Error: " + (1.0 - (double) intersection.size() / union.size()));
                result.forEach(System.out::println);
            }
        }

        System.out.println("Queries took: " + (System.currentTimeMillis() - start));

        new Scanner(System.in).nextLine();
        instance.shutdown();
    }

    private static int computeIntersection(Set<String> tokenSet, Post post) {
        int intersection = 0;
        for (String token : post.tokens) {
            if (tokenSet.contains(token)) {
                ++intersection;
            }
        }
        return intersection;
    }

}

