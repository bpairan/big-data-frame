package com.cuemargin.big.dataframe.source.builder

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
trait BatchSizeBuilder {
def inBatchOf(batchSize: Int): OutputBuilder
}
