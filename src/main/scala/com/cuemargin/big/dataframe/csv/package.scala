package com.cuemargin.big.dataframe

import cats.data.ValidatedNel

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
package object csv {
  type CsvParseErrorOr[T] = ValidatedNel[CsvParseError, T]

  sealed abstract class CsvParseError(val message: String)

  case class FileNotFound(override val message: String) extends CsvParseError(message)

  case class LineParseError(idx: Int, override val message: String) extends CsvParseError(message)

  case class FileParseError(override val message: String) extends CsvParseError(message)
}
