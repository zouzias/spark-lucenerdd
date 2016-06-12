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

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.ar.ArabicAnalyzer
import org.apache.lucene.analysis.bg.BulgarianAnalyzer
import org.apache.lucene.analysis.br.BrazilianAnalyzer
import org.apache.lucene.analysis.ca.CatalanAnalyzer
import org.apache.lucene.analysis.cjk.CJKAnalyzer
import org.apache.lucene.analysis.ckb.SoraniAnalyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.analysis.cz.CzechAnalyzer
import org.apache.lucene.analysis.da.DanishAnalyzer
import org.apache.lucene.analysis.de.GermanAnalyzer
import org.apache.lucene.analysis.el.GreekAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.es.SpanishAnalyzer
import org.apache.lucene.analysis.eu.BasqueAnalyzer
import org.apache.lucene.analysis.fa.PersianAnalyzer
import org.apache.lucene.analysis.fi.FinnishAnalyzer
import org.apache.lucene.analysis.fr.FrenchAnalyzer
import org.apache.lucene.analysis.ga.IrishAnalyzer
import org.apache.lucene.analysis.gl.GalicianAnalyzer
import org.apache.lucene.analysis.hi.HindiAnalyzer
import org.apache.lucene.analysis.hu.HungarianAnalyzer
import org.apache.lucene.analysis.id.IndonesianAnalyzer
import org.apache.lucene.analysis.it.ItalianAnalyzer
import org.apache.lucene.analysis.lt.LithuanianAnalyzer
import org.apache.lucene.analysis.lv.LatvianAnalyzer
import org.apache.lucene.analysis.nl.DutchAnalyzer
import org.apache.lucene.analysis.no.NorwegianAnalyzer
import org.apache.lucene.analysis.pt.PortugueseAnalyzer
import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tr.TurkishAnalyzer
import org.zouzias.spark.lucenerdd.config.Configurable

trait AnalyzerConfigurable extends Configurable {

  val AnalyzerConfigKey = "lucenerdd.analyzer.name"

  /** Default value for topK queries */
  protected val Analyzer: Analyzer = {
    if (config.hasPath(AnalyzerConfigKey)) {
      val analyzerName = config.getString(AnalyzerConfigKey)

      analyzerName match {
        case "whitespace" => new WhitespaceAnalyzer()
        case "ar" => new ArabicAnalyzer()
        case "bg" => new BulgarianAnalyzer()
        case "br" => new BrazilianAnalyzer()
        case "ca" => new CatalanAnalyzer()
        case "cjk" => new CJKAnalyzer()
        case "ckb" => new SoraniAnalyzer()
        case "cz" => new CzechAnalyzer()
        case "da" => new DanishAnalyzer()
        case "de" => new GermanAnalyzer()
        case "el" => new GreekAnalyzer()
        case "en" => new EnglishAnalyzer()
        case "es" => new SpanishAnalyzer()
        case "eu" => new BasqueAnalyzer()
        case "fa" => new PersianAnalyzer()
        case "fi" => new FinnishAnalyzer()
        case "fr" => new FrenchAnalyzer()
        case "ga" => new IrishAnalyzer()
        case "gl" => new GalicianAnalyzer()
        case "hi" => new HindiAnalyzer()
        case "hu" => new HungarianAnalyzer()
        case "id" => new IndonesianAnalyzer()
        case "it" => new ItalianAnalyzer()
        case "lt" => new LithuanianAnalyzer()
        case "lv" => new LatvianAnalyzer()
        case "nl" => new DutchAnalyzer()
        case "no" => new NorwegianAnalyzer()
        case "pt" => new PortugueseAnalyzer()
        case "ru" => new RussianAnalyzer()
        case "tr" => new TurkishAnalyzer()
        case _ => new StandardAnalyzer()
      }
    }
    else {
      new StandardAnalyzer()
    }
  }
}
