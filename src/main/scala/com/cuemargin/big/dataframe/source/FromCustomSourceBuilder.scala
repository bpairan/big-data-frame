package com.cuemargin.big.dataframe.source

import com.cuemargin.big.dataframe.source.builder.{BatchSizeBuilder, OutputBuilder, SourceBuilder}
import org.apache.arrow.vector.ipc.ArrowFileWriter

import java.io.OutputStream
import java.util
import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
case class FromCustomSourceBuilder[T] private[source](sourceSchema: SourceSchema[T],
                                                      sourceIterator: Iterator[T] = Iterator.empty,
                                                      batchSize: Int = 1024)
  extends SourceBuilder[T]
  with BatchSizeBuilder
  with OutputBuilder {

  private def toArrowWriterProvider(arrowFormat: ArrowFormat): ArrowWriterProvider = {
    arrowFormat match {
      case ArrowFormat.Stream => throw new UnsupportedOperationException("stream format not supported")
      case ArrowFormat.File => (schemaRoot, dictProvider, writeChannel) => new ArrowFileWriter(schemaRoot, dictProvider, writeChannel)
    }
  }

  override def withSource(it: Iterator[T]): BatchSizeBuilder = this.copy(sourceIterator = it)

  override def withSource(it: util.Iterator[T]): BatchSizeBuilder = withSource(it.asScala)

  override def inBatchOf(batchSize: Int): OutputBuilder = this.copy(batchSize = batchSize)

  override def writeInto(outStream: OutputStream, arrowFormat: ArrowFormat): Unit = {
    val recordBatchWriter = sourceSchema.toRecordBatchWriter(batchSize, outStream, toArrowWriterProvider(arrowFormat))
    sourceIterator.foreach(recordBatchWriter.write)
    recordBatchWriter.close()
  }
}
