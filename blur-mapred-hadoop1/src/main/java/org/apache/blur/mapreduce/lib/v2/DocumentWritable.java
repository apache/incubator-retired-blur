/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.mapreduce.lib.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;

public class DocumentWritable implements Writable {

  private static final String UTF_8 = "UTF-8";
  private List<IndexableField> _document = new ArrayList<IndexableField>();

  public DocumentWritable() {

  }

  public DocumentWritable(List<IndexableField> document) {
    _document = document;
  }

  public void clear() {
    _document.clear();
  }

  public void add(IndexableField field) {
    _document.add(field);
  }

  public DocumentWritable(Document document) {
    _document = document.getFields();
  }

  public List<IndexableField> getDocument() {
    return _document;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int numberOfFields = _document.size();
    WritableUtils.writeVInt(out, numberOfFields);
    for (int i = 0; i < numberOfFields; i++) {
      IndexableField field = _document.get(i);
      write(out, field);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    _document.clear();
    int numberOfFields = WritableUtils.readVInt(in);
    for (int i = 0; i < numberOfFields; i++) {
      IndexableField field = readField(in);
      _document.add(field);
    }
  }

  private IndexableField readField(DataInput in) throws IOException {
    LUCENE_FIELD_TYPE type = LUCENE_FIELD_TYPE.lookupByValue(in.readByte());
    String name = readString(in);
    switch (type) {
    case StringField:
      return readStringField(in, name);
    default:
      throw new IOException("Type [" + type + "] not supported.");
    }
  }

  private void write(DataOutput out, IndexableField field) throws IOException {
    LUCENE_FIELD_TYPE type = LUCENE_FIELD_TYPE.lookupByClass(field.getClass());
    out.writeByte(type.value());
    writeString(out, field.name());
    switch (type) {
    case StringField:
      writeStringField(out, (StringField) field);
      return;
    default:
      throw new IOException("Type [" + type + "] not supported.");
    }
  }

  private void writeStringField(DataOutput out, StringField stringField) throws IOException {
    FieldType fieldType = stringField.fieldType();
    if (fieldType.equals(StringField.TYPE_STORED)) {
      out.writeBoolean(true);
    } else if (fieldType.equals(StringField.TYPE_NOT_STORED)) {
      out.writeBoolean(false);
    } else {
      throw new IOException("Non default FieldTypes for StringField not supported.");
    }
    writeString(out, stringField.stringValue());
  }

  private IndexableField readStringField(DataInput in, String name) throws IOException {
    boolean stored = in.readBoolean();
    String value = readString(in);
    if (stored) {
      return new StringField(name, value, Store.YES);
    } else {
      return new StringField(name, value, Store.NO);
    }
  }

  private String readString(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    byte[] buf = new byte[length];
    in.readFully(buf);
    return new String(buf, UTF_8);
  }

  private void writeString(DataOutput out, String value) throws IOException {
    byte[] bs = value.getBytes(UTF_8);
    WritableUtils.writeVInt(out, bs.length);
    out.write(bs);
  }

}
