package com.cuemargin.big.dataframe

import okio.ByteString
import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot}

import java.io.File
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.control.Breaks.{break, breakable}

case class DataFrame[IOStyle](file: File) extends IndexedSeq[DataFrameRow] with AutoCloseable {

  val readArrow: ReadArrow = ReadArrow(file)

  // works
  override def headOption: Option[DataFrameRow] = {
    val (reader, root) = readArrow.newStreamReader
    if (reader.loadNextBatch()) {
      Some(DataFrameRow(toData(root, 0)))
    } else {
      None
    }
  }

  private def toData(root: VectorSchemaRoot, rowIdx: Int) = {
    val row = root.getFieldVectors.asScala.map(_.asInstanceOf[VarCharVector]).map(v => {
      val bytes = v.get(rowIdx)
      v.getField.getName -> ByteString.of(bytes, 0, bytes.length)
    })
    ListMap(row.toSeq: _*)
  }

  // works
  override def length: Int = {
    val (reader, root) = readArrow.newStreamReader
    var count = 0
    while (reader.loadNextBatch()) {
      count += root.getRowCount
    }
    count
  }

  override def apply(idx: Int): DataFrameRow = {
    val (reader, root) = readArrow.newStreamReader
    var rowIdx = idx
    var batchCount = 0;
    breakable {
      while (reader.loadNextBatch()) {
        batchCount += 1
        if (rowIdx <= root.getRowCount - 1) {
          break
        } else {
          rowIdx -= root.getRowCount
        }
      }
    }
    DataFrameRow(toData(root, rowIdx))
  }

  override def close(): Unit = {
    readArrow.close()
  }
}

