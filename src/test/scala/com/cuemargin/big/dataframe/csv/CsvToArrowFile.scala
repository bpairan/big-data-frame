package com.cuemargin.big.dataframe.csv

import com.cuemargin.big.dataframe.source.{ArrowFormat, From, SourceSchemaBuilder}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}

import java.io.{File, FileOutputStream}
import java.nio.file.Paths

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
object CsvToArrowFile {

  def main(args: Array[String]): Unit = {

    val sourceSchema = SourceSchemaBuilder.newInstance[Seq[String]]()
      .addField("Series_reference", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq.head)
      .addField("Period", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq(1))
      .addField("Data_value", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), strSeq => {
        if (strSeq(2).nonEmpty) strSeq(2).toDouble else null
      })
      .addField("Suppressed", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq(3))
      .addField("STATUS", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq(4))
      .addField("UNITS", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq(5))
      .addField("Magnitude", FieldType.nullable(new ArrowType.Int(32, false)), strSeq => if (strSeq(6).nonEmpty) strSeq(6).toInt else null)
      .addField("Subject", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq(7))
      .addField("Group", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq(8))
      .addField("Series_title_1", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq(9))
      .addField("Series_title_2", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq(10))
      .addField("Series_title_3", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq(11))
      .addField("Series_title_4", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq(12))
//      .addField("Series_title_5", FieldType.nullable(new ArrowType.Utf8()), strSeq => strSeq(13))
      .build()

    val it = new CsvFileIterator(Paths.get("/Users/barathi/projects/big-data-frame/src/main/resources/electronic-card-transactions-february-2023-csv-tables.csv"), ',', '"')

    val outFile = new File("/Users/barathi/projects/big-data-frame/electronic-card-transactions-february-2023.arrow")
    outFile.createNewFile()

    From.schema(sourceSchema)
      .withSource(it)
      .inBatchOf(2048)
      .writeInto(new FileOutputStream(outFile), ArrowFormat.File)
  }

}
