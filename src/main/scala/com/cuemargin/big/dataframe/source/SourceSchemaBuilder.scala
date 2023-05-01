package com.cuemargin.big.dataframe.source

import org.apache.arrow.vector.types.pojo.{Field, FieldType, Schema}

import java.util.function.{Function => JavaFunction}
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.FunctionConverters.enrichAsScalaFromFunction

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
case class SourceSchemaBuilder[T](fieldFunctions: List[FieldFunction[T]]) {

  def addField(fieldName: String, fieldType: FieldType, mappingFn: Function[T, _]): SourceSchemaBuilder[T] = {
    this.copy(fieldFunctions :+ FieldFunction(new Field(fieldName, fieldType, null), mappingFn))
  }

  def addField(fieldName: String, fieldType: FieldType, mappingFn: JavaFunction[T, _]): SourceSchemaBuilder[T] = {
    addField(fieldName, fieldType, mappingFn.asScala)
  }

  def build(): SourceSchema[T] = {
    SourceSchema(new Schema(fieldFunctions.map(_.field).asJava), fieldFunctions.map(fn => fn.field.getName -> fn.mappingFn).toMap)
  }
}

object SourceSchemaBuilder {
  def newInstance[S](): SourceSchemaBuilder[S] = new SourceSchemaBuilder[S](List.empty)
}
