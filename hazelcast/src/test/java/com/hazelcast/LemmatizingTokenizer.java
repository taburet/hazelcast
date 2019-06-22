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

import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.lemmatizer.LemmatizerME;
import opennlp.tools.lemmatizer.LemmatizerModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public class LemmatizingTokenizer {

    private static final Set<String> TAG_BLACKLIST;
    private static final Set<String> TAG_WHITELIST;

    static {
        // https://www.ling.upenn.edu/courses/Fall_2003/ling001/penn_treebank_pos.html
        TAG_BLACKLIST = new HashSet<>();
        TAG_BLACKLIST.add(","); // punctuation
        TAG_BLACKLIST.add("."); // end of sentence: .?!
        TAG_BLACKLIST.add(":"); // pauses: ...
        TAG_BLACKLIST.add("''"); // '
        TAG_BLACKLIST.add("``"); // "
        TAG_BLACKLIST.add("#"); // #
        TAG_BLACKLIST.add("$"); // $
        TAG_BLACKLIST.add("RB"); // adverb: badly, far, little, well, ...
        TAG_BLACKLIST.add("DT"); // determiner: a, the, all, this, ...
        TAG_BLACKLIST.add("PRP"); // personal pronoun: I, you, he, she, ...
        TAG_BLACKLIST.add("IN"); // preposition or subordinating conjunction: of, by, ...
        TAG_BLACKLIST.add("TO"); // to
        TAG_BLACKLIST.add("MD"); // modal: can, might, will, ...
        TAG_BLACKLIST.add("PRP$"); // possessive pronoun: yours, mine, theirs, ...
        TAG_BLACKLIST.add("CC"); // coordinating conjunction: and, or, ...
        TAG_BLACKLIST.add("WRB"); // wh-adverb: how, when, why, ...
        TAG_BLACKLIST.add("EX"); // existential there: there is something...
        TAG_BLACKLIST.add("POS"); // possessive ending: Sergey"'s"
        TAG_BLACKLIST.add("JJS"); // adjective, superlative: fastest, strongest, ...
        TAG_BLACKLIST.add("WDT"); // wh-determiner: that
        TAG_BLACKLIST.add("SYM"); // symbol (?)
        TAG_BLACKLIST.add("RP"); // particle: in, up, out, ...
        TAG_BLACKLIST.add("JJR"); // adjective, comparative: harder, better, faster, stronger, ...
        TAG_BLACKLIST.add("-LRB-"); // (
        TAG_BLACKLIST.add("-RRB-"); // )
        TAG_BLACKLIST.add("UH"); // interjection: uh, oh, ah, ...
        TAG_BLACKLIST.add("WP"); // wh-pronoun: who, what, which, ...
        TAG_BLACKLIST.add("RBR"); // adverb, comparative: more, worse, less, ...
        TAG_BLACKLIST.add("PDT"); // predeterminer: both, all, ...
        TAG_BLACKLIST.add("LS"); // list item marker
        TAG_BLACKLIST.add("RBS"); // adverb, superlative: worst, best, ...
        TAG_BLACKLIST.add("WP$"); // possessive wh-pronoun: who, which, ...

        TAG_WHITELIST = new HashSet<>();
        TAG_WHITELIST.add("NN"); // noun, singular or mass
        TAG_WHITELIST.add("VBZ"); // verb, 3rd person singular present
        TAG_WHITELIST.add("VBD"); // verb, past tense
        TAG_WHITELIST.add("NNP"); // proper noun, singular: John, London
        TAG_WHITELIST.add("VB"); // verb, base form
        TAG_WHITELIST.add("JJ"); // adjective: sweet, red, technical, ...
        TAG_WHITELIST.add("VBG"); // verb, gerund or present participle: running, seeking, ...
        TAG_WHITELIST.add("NNS"); // noun, plural
        TAG_WHITELIST.add("CD"); // cardinal number: 1, 50, two, ...
        TAG_WHITELIST.add("VBP"); // verb, non-3rd person singular present
        TAG_WHITELIST.add("VBN"); // verb, past participle
        TAG_WHITELIST.add("FW"); // foreign word
        TAG_WHITELIST.add("NNPS"); // proper noun, plural
    }

    private final SentenceDetectorME sentenceDetector;
    private final TokenizerME tokenizer;
    private final POSTaggerME posTagger;
    private final LemmatizerME statisticalLemmatizer;
    private final DictionaryLemmatizer dictionaryLemmatizer;

    private int sentenceIndex;
    private int tagIndex;

    private String[] sentences;
    private String[] tokens;
    private String[] tags = {};
    private String[] statisticalLemmas;
    private String[] dictionaryLemmas;

    public LemmatizingTokenizer() {
        // http://opennlp.sourceforge.net/models-1.5/
        try {
            SentenceModel sentenceModel = new SentenceModel(Paths.get("/Users/sergey/Downloads/en-sent.bin"));
            this.sentenceDetector = new SentenceDetectorME(sentenceModel);

            TokenizerModel tokenizerModel = new TokenizerModel(Paths.get("/Users/sergey/Downloads/en-token.bin"));
            this.tokenizer = new TokenizerME(tokenizerModel);

            POSModel posModel = new POSModel(Paths.get("/Users/sergey/Downloads/en-pos-maxent.bin"));
            this.posTagger = new POSTaggerME(posModel);

            // https://github.com/JiNova/Java2-Project
            LemmatizerModel lemmatizerModel = new LemmatizerModel(Paths.get("/Users/sergey/Downloads/en-lemmatizer.bin"));
            this.statisticalLemmatizer = new LemmatizerME(lemmatizerModel);

            // https://github.com/richardwilly98/elasticsearch-opennlp-auto-tagging/tree/master/src/main/resources/models
            this.dictionaryLemmatizer = new DictionaryLemmatizer(Paths.get("/Users/sergey/Downloads/en-lemmatizer.dict.txt"));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setText(String text) {
        sentences = sentenceDetector.sentDetect(text);
        sentenceIndex = 0;
        tagIndex = Integer.MAX_VALUE;
    }

    public String next() {
        while (true) {
            if (tagIndex >= tags.length) {
                if (sentenceIndex >= sentences.length) {
                    return null;
                }

                tokens = tokenizer.tokenize(sentences[sentenceIndex]);
                tags = posTagger.tag(tokens);
                statisticalLemmas = null; //statisticalLemmas = statisticalLemmatizer.lemmatize(tokens, tags);
                dictionaryLemmas = dictionaryLemmatizer.lemmatize(tokens, tags);

                tagIndex = 0;
                ++sentenceIndex;
            }

            String tag = tags[tagIndex];

            if (!TAG_BLACKLIST.contains(tag)) {
//                if (!TAG_WHITELIST.contains(tag)) {
//                    System.out.println(tag + ": " + lemma);
//                }

                String lemma;
                if (dictionaryLemmas[tagIndex].equals("O")) {
                    if (statisticalLemmas == null) {
                        statisticalLemmas = statisticalLemmatizer.lemmatize(tokens, tags);
                    }
                    lemma = statisticalLemmas[tagIndex];
                } else {
                    lemma = dictionaryLemmas[tagIndex];
                }

                ++tagIndex;
                return lemma + "_" + tag.charAt(0);
            }

            ++tagIndex;

        }
    }

}
