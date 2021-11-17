package com.cuemargin.big.dataframe

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.pojo.Schema
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileOutputStream}

/**
 * Created by Bharathi Pairan on 11/11/2021.
 */
class ChunkedFileWriter[T](val writeVectorFn: WriteVector[T],
                           val columnNames: Array[String],
                           val schema: Schema,
                           file: File) extends ChunkedWriter[T] {

  val log: Logger = LoggerFactory.getLogger(getClass)

  val dictProvider = new DictionaryProvider.MapDictionaryProvider
  val allocator = new RootAllocator()
  val schemaRoot: VectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
  val fd = new FileOutputStream(file)
  val writer = new ArrowFileWriter(schemaRoot, dictProvider, fd.getChannel)
  writer.start()
  log.info("Start Writing")

  override def close(): Unit = {
    super.close()
    fd.close()
    schemaRoot.close()
    allocator.close()
  }
}
