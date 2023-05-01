package com.cuemargin.big.dataframe.csv

import cats.data.Chain
import cats.implicits._

import java.nio.file.Path
import scala.io.{BufferedSource, Source}

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
class CsvFileIterator(filePath: Path, separator: Char, quote: Char, skipHeader: Boolean = true) extends Iterator[Seq[String]] with AutoCloseable {
  private val source: BufferedSource = Source.fromFile(filePath.toFile)
  private val sourceIterator: Iterator[String] = source.getLines()

  var idx = 0

  override def hasNext: Boolean = sourceIterator.hasNext

  override def next(): Seq[String] = {
    val line = sourceIterator.next()
    idx += 1
    if (idx == 1 && skipHeader) {
      next()
    } else {
      parse(idx, line).fold(_ => Seq.empty, identity)
    }
  }

  private def parse(idx: Int, line: String): CsvParseErrorOr[Seq[String]] = {
    var startQuote = false
    var endQuote = false
    val builder = new StringBuilder(line.length)
    //    val result = mutable.ListBuffer[String]()
    var result = Chain.empty[String]
    try {
      for (c <- line) {
        c match {
          // It's a start quote
          case q@_ if q == quote && !startQuote => startQuote = true
          // It's an end quote
          case q@_ if q == quote && startQuote => endQuote = true
          // hit the separator with or without quotes, so push the built string to result
          case s@_ if s == separator && ((startQuote && endQuote) || (!startQuote && !endQuote)) =>
            result = result.append(builder.toString().trim)
            builder.clear()
            startQuote = false
            endQuote = false
          // hit separator without end quote so separator is part of the result
          case s@_ if s == separator && startQuote && !endQuote => builder.append(separator)
          // add
          case s@_ => builder.append(s)
        }
      }

      onLastLine(builder, result).toList.validNel
    } catch {
      case t: Throwable => LineParseError(idx, t.getMessage).invalidNel
    }
  }

  private def onLastLine(builder: StringBuilder, result: Chain[String]): Chain[String] = {
    val lastCol = builder.toString().trim
    if (lastCol.nonEmpty) {
      val lastSeparatorIdx = lastCol.lastIndexOf(separator)
      // strip trailing separator
      if (lastSeparatorIdx != -1 && lastSeparatorIdx == lastCol.length - 1) {
        result.append(lastCol.substring(0, lastSeparatorIdx))
      } else {
        result.append(lastCol)
      }
    } else {
      result
    }
  }

  override def close(): Unit = source.close()
}
