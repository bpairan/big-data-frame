package com.cuemargin.big.dataframe.source.builder

import com.cuemargin.big.dataframe.source.ArrowFormat

import java.io.OutputStream

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
trait OutputBuilder {

  def writeInto(outStream: OutputStream, arrowFormat: ArrowFormat): Unit

}
