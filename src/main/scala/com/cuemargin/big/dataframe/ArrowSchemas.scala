package com.cuemargin.big.dataframe

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import scala.collection.JavaConverters.asJavaIterableConverter

/**
 * Created by Bharathi Pairan on 11/11/2021.
 */
object ArrowSchemas {
  def newSchema(columnsNames: Array[String]): Schema = {
    new Schema(columnsNames.map(name => new Field(name, FieldType.nullable(new ArrowType.Utf8()), null)).toList.asJava)
  }
}
