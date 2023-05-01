package com.cuemargin.big.dataframe

import java.io.File

/**
 * Created by Bharathi Pairan on 11/11/2021.
 */
object CsvToArrowFile extends App with CsvToArrow {

  val chunkedWriterProvider = (columnNames: Array[String], outputFilePath: String) => new ChunkedFileWriter[Array[String]](writeVector, columnNames, ArrowSchemas.newSchema(columnNames), new File(outputFilePath))
  //parse(inputFilePath = "Data8277.csv", outputFilePath = "data8277.arrow", chunkedWriterProvider)
  parse(inputFilePath = "electronic-card-transactions-february-2023-csv-tables.csv", outputFilePath = "electronic-card-transactions-february-2023.arrow", chunkedWriterProvider, 10_000)
}
