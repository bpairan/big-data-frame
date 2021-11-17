package com.cuemargin.big.dataframe

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.Schema
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileOutputStream}
import java.nio.channels.Channels

/**
 * Created by Bharathi Pairan on 12/11/2021.
 */
class ChunkedStreamWriter[T](val writeVectorFn: WriteVector[T],
                             val columnNames: Array[String],
                             val schema: Schema,
                             file: File) extends ChunkedWriter[T] {

  val log: Logger = LoggerFactory.getLogger(getClass)
  val allocator = new RootAllocator()
  val out = new FileOutputStream(file)

  val schemaRoot: VectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
  val writer = new ArrowStreamWriter(schemaRoot, null, Channels.newChannel(out))
  writer.start()
  log.info("Start Writing")

  override def close(): Unit = {
    super.close()
    out.close()
    schemaRoot.close()
    allocator.close()
  }
}
