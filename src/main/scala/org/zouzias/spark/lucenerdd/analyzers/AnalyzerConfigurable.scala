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
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Lucene Analyzer loader via configuration or via class name
  *
  * An analyzer can be loaded by using all the short country codes, i.e.,
  * en,el,de, etc or using a class name present in the classpath, i.e.,
  * 'org.apache.lucene.analysis.el.GreekAnalyzer'
  *
  * Custom Analyzers can be loaded provided that are present during runtime.
 */
trait AnalyzerConfigurable extends Configurable
  with Logging {

  private val IndexAnalyzerConfigKey = "lucenerdd.index.analyzer.name"
  private val QueryAnalyzerConfigKey = "lucenerdd.query.analyzer.name"

  /** Get the configured analyzers or fallback to English */
  protected def getOrElseEn(analyzerName: Option[String]): String = analyzerName.getOrElse("en")

  protected val IndexAnalyzerConfigName: Option[String] =
    if (Config.hasPath(IndexAnalyzerConfigKey)) {
    Some(Config.getString(IndexAnalyzerConfigKey))} else None

  protected val QueryAnalyzerConfigName: Option[String] =
    if (Config.hasPath(QueryAnalyzerConfigKey)) {
      Some(Config.getString(QueryAnalyzerConfigKey))} else None

  protected def getAnalyzer(analyzerName: Option[String]): Analyzer = {
    if (analyzerName.isDefined) {
      analyzerName.get match {
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
        case otherwise: String =>
          try {
            val clazz = loadConstructor[Analyzer](otherwise)
            clazz
          }
          catch {
            case e: ClassNotFoundException =>
              logError(s"Class ${otherwise} was not found in classpath. Does the class exist?", e)
              null
            case e: ClassCastException =>
              logError(s"Class ${otherwise} could not be " +
                s"cast to superclass org.apache.lucene.analysis.Analyzer.", e)
              null
            case e: Throwable =>
              logError(s"Class ${otherwise} could not be used as Analyzer.", e)
              null
          }
      }
    }
    else {
      logInfo("Analyzer name is not defined. Default analyzer is StandardAnalyzer().")
      new StandardAnalyzer()
    }
  }

  /**
    * Load a Lucene [[Analyzer]] using class name
    *
    * @param className The class name of the analyzer to load
    * @tparam T
    * @return Returns a Lucene Analyzer
    */
  private def loadConstructor[T <: Analyzer](className: String): T = {
    val loader = getClass.getClassLoader
    logInfo(s"Loading class ${className} using loader ${loader}")
    val loadedClass: Class[T] = loader.loadClass(className).asInstanceOf[Class[T]]
    val constructor = loadedClass.getConstructor()
    constructor.newInstance()
  }

}
