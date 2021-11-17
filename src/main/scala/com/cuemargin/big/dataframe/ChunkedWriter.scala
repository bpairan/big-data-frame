package com.cuemargin.big.dataframe

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowWriter
import org.apache.arrow.vector.types.pojo.Schema
import org.slf4j.Logger

/**
 * Created by Bharathi Pairan on 12/11/2021.
 */
trait ChunkedWriter[T] extends AutoCloseable {
  val log: Logger
  val writeVectorFn: WriteVector[T]
  val columnNames: Array[String]
  val schema: Schema

  val writer: ArrowWriter
  val schemaRoot: VectorSchemaRoot

  def write(values: Seq[T]): Unit = {
    schemaRoot.allocateNew() // allocate once per batch

    var index = 0
    values.foreach { value =>
      writeVectorFn(value, index, columnNames, schemaRoot)
      index += 1
    }

    schemaRoot.setRowCount(index)
    log.info("Begin writing batch")
    writer.writeBatch()
    log.info("End writing batch")
    schemaRoot.clear()
  }

  override def close(): Unit = {
    writer.end()
    log.info("End Writing")
    writer.close()
  }
}
