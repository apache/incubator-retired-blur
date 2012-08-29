package com.nearinfinity.blur.analysis;

import org.apache.lucene.document.Fieldable;

public interface FieldConverter {

  Fieldable convert(Fieldable orig);

}
