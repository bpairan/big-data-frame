package com.cuemargin.big.dataframe.source

import org.apache.arrow.vector.types.pojo.Schema

import java.io.OutputStream

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
case class SourceSchema[T](schema: Schema, mappingFunctions: Map[String, Function[T, _]]) {

  def toRecordBatchWriter(batchSize: Int, outStream: OutputStream, arrowWriterProvider: ArrowWriterProvider): RecordBatchWriter[T] = {
    new RecordBatchWriter[T](this, batchSize, outStream, arrowWriterProvider)
  }
}
