package com.cuemargin.big.dataframe.source

import com.cuemargin.big.dataframe.source.RecordBatchBase.WriteInfo
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
abstract class RecordBatchBase[T](sourceSchema: SourceSchema[T], batchSize: Int) {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  private val allocator = new RootAllocator()
  protected val schemaRoot: VectorSchemaRoot = VectorSchemaRoot.create(sourceSchema.schema, allocator)

  private val schemaOrderedWriteInfoSeq: Seq[WriteInfo[T]] = sourceSchema.schema.getFields.asScala.map { field =>
    val vectorField = schemaRoot.getVector(field)
    vectorField.setInitialCapacity(batchSize)
    vectorField.allocateNew()
    WriteInfo[T](field.getType, vectorField, sourceSchema.mappingFunctions.get(field.getName))
  }.toSeq

  protected val batchCount = new AtomicInteger(0)
  private val noOfBatches = new AtomicInteger(0)

  def executePostBatch(): Unit

  def write(value: T): Unit = {
    // End the batch when batchSize is reached
    if (batchCount.intValue() == batchSize) flushBatchAndBegin()
    fillVectorSchemaRoot(value, batchCount.getAndIncrement())
  }

  def fillVectorSchemaRoot(source: T, idx: Int): Unit = {
    schemaOrderedWriteInfoSeq.foreach { writeInfo =>
      val value = writeInfo.fn.map(_(source))
      ArrowTypedWriter.typedWrite(writeInfo.arrowType, writeInfo.fieldVector, idx, value)
    }
  }

  protected def endBatch(): Unit = {
    schemaRoot.setRowCount(batchCount.intValue())
    executePostBatch()
    schemaRoot.clear()
  }

  private def flushBatchAndBegin(): Unit = {
    log.info(s"Flushing batch ${noOfBatches.incrementAndGet()} with $batchSize values")
    endBatch()
    batchCount.set(0)
  }
}

object RecordBatchBase {
  private case class WriteInfo[T](arrowType: ArrowType, fieldVector: FieldVector, fn: Option[Function[T, _]])
}
