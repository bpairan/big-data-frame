package com.cuemargin.big.dataframe.source.builder

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
trait SourceBuilder[T] {

  def withSource(it: Iterator[T]): BatchSizeBuilder

  def withSource(it: java.util.Iterator[T]): BatchSizeBuilder
}
