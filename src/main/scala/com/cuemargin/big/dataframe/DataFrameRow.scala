package com.cuemargin.big.dataframe

import okio.ByteString

import java.nio.charset.Charset

/**
 * Created by Bharathi Pairan on 11/11/2021.
 */
case class DataFrameRow(data: Map[String, ByteString]) {
  def getConverted(colName: String): Option[String] = data.get(colName).map(_.string(Charset.defaultCharset()))
}
