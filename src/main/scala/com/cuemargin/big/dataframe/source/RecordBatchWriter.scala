package com.cuemargin.big.dataframe.source

import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.ArrowWriter
import org.slf4j.{Logger, LoggerFactory}

import java.io.OutputStream
import java.nio.channels.Channels

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
class RecordBatchWriter[T](sourceSchema: SourceSchema[T],
                           val batchSize: Int,
                           outStream: OutputStream,
                           arrowWriterProvider: ArrowWriterProvider,
                           dictProvider: DictionaryProvider = new DictionaryProvider.MapDictionaryProvider())
  extends RecordBatchBase[T](sourceSchema, batchSize) with AutoCloseable {

  private val log: Logger = LoggerFactory.getLogger(getClass)
  private val writer: ArrowWriter = arrowWriterProvider(schemaRoot, dictProvider, Channels.newChannel(outStream))

  writer.start()
  log.info("Start Writing")

  override def executePostBatch(): Unit = {
    log.debug(s"Begin writing batch of ${batchCount.intValue()} values")
    writer.writeBatch()
    log.debug(s"End writing batch of ${batchCount.intValue()} values")
  }

  override def close(): Unit = {
    if (batchCount.intValue() > 0) {
      log.info(s"Flushing ${batchCount.intValue()} values as final batch")
      endBatch()
    }
    writer.end()
    log.info("End Writing")
    writer.close()
  }
}
