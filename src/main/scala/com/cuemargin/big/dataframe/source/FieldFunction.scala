package com.cuemargin.big.dataframe.source

import org.apache.arrow.vector.types.pojo.Field

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
case class FieldFunction[S](field: Field, mappingFn: Function[S, _])
