package com.cuemargin.big

import org.apache.arrow.vector.VectorSchemaRoot

/**
 * Created by Bharathi Pairan on 12/11/2021.
 */
package object dataframe {
  type WriteVector[T] = (T, Int, Array[String], VectorSchemaRoot) => Unit

  type ChunkedWriterProvider = (Array[String], String) => ChunkedWriter[Array[String]]

  sealed trait IOStyle

  case object Stream extends IOStyle

  case object File extends IOStyle
}
