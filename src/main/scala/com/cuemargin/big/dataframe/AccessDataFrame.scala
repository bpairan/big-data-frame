package com.cuemargin.big.dataframe

import org.apache.commons.lang3.time.StopWatch
import org.slf4j.LoggerFactory

import java.io.File
import scala.util.Using

/**
 * Created by Bharathi Pairan on 11/11/2021.
 */
object AccessDataFrame extends App {

  val log = LoggerFactory.getLogger(getClass)
  val stopWatch = new StopWatch()

  def printLength(df: DataFrame[_]): Unit = {
    log.info(s"Length: ${df.length}")
  }

  def printHead(df: DataFrame[_]): Unit = {
    val headOption = df.headOption
    log.info(s"Head -> $headOption")
    log.info(s"Year: ${headOption.map(_.getConverted("Year"))}")
    log.info(s"Age: ${headOption.map(_.getConverted("Age"))}")
    log.info(s"Ethnic: ${headOption.map(_.getConverted("Ethnic"))}")
    log.info(s"Sex: ${headOption.map(_.getConverted("Sex"))}")
    log.info(s"Area: ${headOption.map(_.getConverted("Area"))}")
    log.info(s"count: ${headOption.map(_.getConverted("count"))}")
  }

  def printTopN(df: DataFrame[_], topN: Int): Unit = {
    val value: Seq[DataFrameRow] = df.take(topN)
    log.info(s"$value")
  }

  stopWatch.start()

  Using(DataFrame[Stream.type](new File("data8277_stream.arrow"))) { df =>
    printTopN(df, 5)
    log.info(s"df(100000) -> ${df(100000)}")
  }

  /*val df = DataFrame(new File("data8277_stream.arrow"))
  log.info(s"Row count: ${df.length}")
  log.info(s"Head -> ${df.length}")
  df.close()*/
  stopWatch.stop()
  log.info(s"Overall timing: $stopWatch")
}
// idx = 19,995
// batchCount = 10,000