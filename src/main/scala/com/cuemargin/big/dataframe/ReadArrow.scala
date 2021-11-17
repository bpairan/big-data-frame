package com.cuemargin.big.dataframe

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowStreamReader, SeekableReadChannel}

import java.io.{File, FileInputStream}

/**
 * Created by Bharathi Pairan on 12/11/2021.
 */
case class ReadArrow(file: File) extends AutoCloseable {

  val allocator = new RootAllocator()
  val inputStream = new FileInputStream(file)

  //For Random Access File reading
  val channel = new SeekableReadChannel(inputStream.getChannel)

  def newStreamReader: (ArrowStreamReader, VectorSchemaRoot) = {
    inputStream.getChannel.position(0)
    val reader = new ArrowStreamReader(inputStream, allocator)
    reader -> reader.getVectorSchemaRoot
  }

  def newFileReader: (ArrowFileReader, VectorSchemaRoot) = {
    channel.setPosition(0)
    val reader = new ArrowFileReader(channel, allocator)
    reader -> reader.getVectorSchemaRoot
  }

  override def close(): Unit = {
    inputStream.close()
  }
}

