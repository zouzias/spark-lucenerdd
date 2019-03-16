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

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class AnalyzersConfigurableSpec extends FlatSpec with Matchers
  with BeforeAndAfterEach
  with AnalyzerConfigurable {

  "AnalyzersConfigurable.getAnalyzer" should "return english analyzer with 'en' input" in {
    val englishAnalyzer = getAnalyzer(Some("en"))
    englishAnalyzer.isInstanceOf[EnglishAnalyzer] should equal(true)
  }

  "AnalyzersConfigurable.getAnalyzer" should
    "return custom test analyzer with 'org.zouzias.spark.lucenerdd.analyzers.TestAnalyzer'" in {
    val englishAnalyzer = getAnalyzer(Some("org.zouzias.spark.lucenerdd.analyzers.TestAnalyzer"))
    englishAnalyzer.isInstanceOf[TestAnalyzer] should equal(true)
  }
}
