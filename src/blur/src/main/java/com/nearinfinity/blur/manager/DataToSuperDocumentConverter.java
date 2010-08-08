package com.nearinfinity.blur.manager;

import java.io.InputStream;

import com.nearinfinity.blur.lucene.index.SuperDocument;

public interface DataToSuperDocumentConverter {

	SuperDocument convert(String mimeType, InputStream inputStream);

}
