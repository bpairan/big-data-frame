package com.cuemargin.big.dataframe

import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot}
import org.apache.commons.lang3.time.StopWatch
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source
import scala.util.Using

/**
 * Created by Bharathi Pairan on 12/11/2021.
 */
trait CsvToArrow {
  val log: Logger = LoggerFactory.getLogger(getClass)


  val defaultChunkSize = 100000
  var count = 0

  def writeVector(row: Array[String], idx: Int, columnsNames: Array[String], schemaRoot: VectorSchemaRoot): Unit = {
    columnsNames.zipWithIndex.foreach { case (columnName, i) =>
      schemaRoot.getVector(columnName).asInstanceOf[VarCharVector].setSafe(idx, row(i).getBytes())
    }
  }

  def parse(inputFilePath: String, outputFilePath: String, chunkedWriterProvider: ChunkedWriterProvider, chunkSize: Int = defaultChunkSize): Unit = {
    val stopWatch: StopWatch = new StopWatch()
    stopWatch.start()
    Using.resource(Source.fromResource(inputFilePath)) { source =>
      //Create a CSV stream from file
      val csvLines = source.getLines().map(_.split(','))

      val columnNames: Array[String] = csvLines.next()
      implicit val chunkedWriter: ChunkedWriter[Array[String]] = chunkedWriterProvider(columnNames, outputFilePath)
      implicit val chunkSize1: Int = chunkSize
      stopWatch.split()

      csvLines
        .grouped(chunkSize)
        .foreach(writeToArrowFile)
      chunkedWriter.close()
      source.close()
    }
    stopWatch.stop()
    log.info(s"Overall timing: $stopWatch")
  }

  private def writeToArrowFile(chunk: Seq[Array[String]])(implicit chunkedWriter: ChunkedWriter[Array[String]], chunkSize: Int): Unit = {
    log.info(s"processing $count to ${count + chunkSize}")
    chunkedWriter.write(chunk)
    count += chunkSize
  }

}
