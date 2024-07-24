/*
 * Copyright 2022 Israel Herraiz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.herraiz.scio

import com.spotify.scio._

object ScioQuickstartPipeline {

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc: ScioContext, args: Args) = ContextAndArgs(cmdLineArgs)
    implicit val scImplicit: ScioContext = sc

    val inputFile: String = args("input-file")
    val numWords: Int = args("num-words").toInt
    val outputFile: String = args("output-file")

    runPipeline(inputFile, numWords, outputFile)
  }

  val thingsToRemove = List('.', ',', '?', '!', '¡', '¿', ';')

  val accents = Map(
    'á' -> 'a',
    'é' -> 'e',
    'í' -> 'i',
    'ó' -> 'o',
    'ú' -> 'u'
  )

  def sanitizeWord(w: String): String = {
    w.toLowerCase.filter(
      c => !thingsToRemove.contains(c
      )).map(
      c => accents.getOrElse(c, c)
    )
  }

  def runPipeline(inputFile: String, numWords: Int, outputFile: String)(implicit sc: ScioContext): Unit = {
    val lines = sc.textFile(inputFile)
    val words = lines.flatMap(
      _.split(" ")
    )
    val clean = words.map(sanitizeWord)

    val counted = clean.countByValue

    val topWords = counted.swap.top(numWords)

    val csvlines = topWords.map{
      t => t.map {
        case (n: Long, w: String) => List(n.toString, w).mkString(",")
      }.mkString("\n")
    }

    csvlines.saveAsTextFile(path=outputFile)

    sc.run()
  }
}
