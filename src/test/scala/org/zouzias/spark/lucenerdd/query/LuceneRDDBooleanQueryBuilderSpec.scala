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
package org.zouzias.spark.lucenerdd.query

import org.apache.lucene.search.{BooleanClause, BooleanQuery}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.zouzias.spark.lucenerdd.builders.{FuzzyQueryBuilder, TermQueryBuilder}

class LuceneRDDBooleanQueryBuilderSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  val luceneRDDBooleanQuery = new LuceneRDDBooleanQueryBuilder()
  val termQueryBuilder: TermQueryBuilder = TermQueryBuilder("foo", "bar")
  val fuzzyQueryBuilder: FuzzyQueryBuilder = FuzzyQueryBuilder("foo", "bar", 2)
  luceneRDDBooleanQuery.add(termQueryBuilder, BooleanClause.Occur.MUST)
  luceneRDDBooleanQuery.add(fuzzyQueryBuilder, BooleanClause.Occur.MUST)

  "LuceneRDDBooleanQuery.buildBooleanQuery" should "return a Lucene Boolean Query" in {
    val booleanQuery : BooleanQuery = luceneRDDBooleanQuery.buildBooleanQuery()
    booleanQuery.clauses().size() should equal(2)
    booleanQuery.clauses().get(0).getQuery.toString() should equal("foo:bar")
    booleanQuery.clauses().get(1).getQuery.toString() should equal("foo:bar~2")
    booleanQuery.toString() should equal ("+foo:bar +foo:bar~2")
  }
}
