package com.cuemargin.big.dataframe.source

import com.cuemargin.big.dataframe.source.builder.SourceBuilder

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
object From {

  def schema[T](sourceSchema: SourceSchema[T]): SourceBuilder[T] = FromCustomSourceBuilder[T](sourceSchema)
}
