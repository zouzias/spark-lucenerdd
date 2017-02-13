/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zouzias.spark.lucenerdd.analyzers

import org.apache.lucene.analysis.{Analyzer, Tokenizer}
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.core.{LowerCaseFilter, WhitespaceTokenizer}
import org.apache.lucene.analysis.ngram.NGramTokenFilter

/**
  * An example with a custom Lucene analyzer (Ngrams and tokenization)
  * Creates NGramTokenizer with given min and max n-grams.
  *
  * @param minGram the smallest n-gram to generate
  * @param maxGram the largest n-gram to generate
  */
class NgramAnalyzer(minGram: Int, maxGram: Int) extends Analyzer {
  override def createComponents(fieldName: String): TokenStreamComponents = {
    val source: Tokenizer = new WhitespaceTokenizer()
    val lowerCase = new LowerCaseFilter(source)
    val ngramSource = new NGramTokenFilter(lowerCase, minGram, maxGram)
    new TokenStreamComponents(source, ngramSource)
  }
}
