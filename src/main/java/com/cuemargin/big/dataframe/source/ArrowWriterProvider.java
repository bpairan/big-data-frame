package com.cuemargin.big.dataframe.source;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowWriter;

import java.nio.channels.WritableByteChannel;

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
@FunctionalInterface
public interface ArrowWriterProvider {

    ArrowWriter apply(VectorSchemaRoot vectorSchemaRoot, DictionaryProvider dictionaryProvider, WritableByteChannel writeChannel);
}
