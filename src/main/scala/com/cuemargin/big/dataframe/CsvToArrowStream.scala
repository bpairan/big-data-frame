package com.cuemargin.big.dataframe

import java.io.File

/**
 * Created by Bharathi Pairan on 12/11/2021.
 */
object CsvToArrowStream extends App with CsvToArrow {
  val chunkedWriterProvider = (columnNames: Array[String], outputFilePath: String) => new ChunkedStreamWriter[Array[String]](writeVector, columnNames, ArrowSchemas.newSchema(columnNames), new File(outputFilePath))
  parse(inputFilePath = "Data8277.csv", outputFilePath = "data8277_stream.arrow", chunkedWriterProvider)
}
