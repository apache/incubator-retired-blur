/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.blur.thrift.generated;

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



import org.apache.blur.thirdparty.thrift_0_9_0.scheme.IScheme;
import org.apache.blur.thirdparty.thrift_0_9_0.scheme.SchemeFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.scheme.StandardScheme;

import org.apache.blur.thirdparty.thrift_0_9_0.scheme.TupleScheme;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TTupleProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocolException;
import org.apache.blur.thirdparty.thrift_0_9_0.EncodingUtils;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class Arguments implements org.apache.blur.thirdparty.thrift_0_9_0.TBase<Arguments, Arguments._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.blur.thirdparty.thrift_0_9_0.protocol.TStruct STRUCT_DESC = new org.apache.blur.thirdparty.thrift_0_9_0.protocol.TStruct("Arguments");

  private static final org.apache.blur.thirdparty.thrift_0_9_0.protocol.TField VALUES_FIELD_DESC = new org.apache.blur.thirdparty.thrift_0_9_0.protocol.TField("values", org.apache.blur.thirdparty.thrift_0_9_0.protocol.TType.MAP, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ArgumentsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ArgumentsTupleSchemeFactory());
  }

  public Map<String,ValueObject> values; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.blur.thirdparty.thrift_0_9_0.TFieldIdEnum {
    VALUES((short)1, "values");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // VALUES
          return VALUES;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.blur.thirdparty.thrift_0_9_0.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.blur.thirdparty.thrift_0_9_0.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.blur.thirdparty.thrift_0_9_0.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.VALUES, new org.apache.blur.thirdparty.thrift_0_9_0.meta_data.FieldMetaData("values", org.apache.blur.thirdparty.thrift_0_9_0.TFieldRequirementType.DEFAULT, 
        new org.apache.blur.thirdparty.thrift_0_9_0.meta_data.MapMetaData(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TType.MAP, 
            new org.apache.blur.thirdparty.thrift_0_9_0.meta_data.FieldValueMetaData(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TType.STRING), 
            new org.apache.blur.thirdparty.thrift_0_9_0.meta_data.StructMetaData(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TType.STRUCT, ValueObject.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.blur.thirdparty.thrift_0_9_0.meta_data.FieldMetaData.addStructMetaDataMap(Arguments.class, metaDataMap);
  }

  public Arguments() {
  }

  public Arguments(
    Map<String,ValueObject> values)
  {
    this();
    this.values = values;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Arguments(Arguments other) {
    if (other.isSetValues()) {
      Map<String,ValueObject> __this__values = new HashMap<String,ValueObject>();
      for (Map.Entry<String, ValueObject> other_element : other.values.entrySet()) {

        String other_element_key = other_element.getKey();
        ValueObject other_element_value = other_element.getValue();

        String __this__values_copy_key = other_element_key;

        ValueObject __this__values_copy_value = new ValueObject(other_element_value);

        __this__values.put(__this__values_copy_key, __this__values_copy_value);
      }
      this.values = __this__values;
    }
  }

  public Arguments deepCopy() {
    return new Arguments(this);
  }

  @Override
  public void clear() {
    this.values = null;
  }

  public int getValuesSize() {
    return (this.values == null) ? 0 : this.values.size();
  }

  public void putToValues(String key, ValueObject val) {
    if (this.values == null) {
      this.values = new HashMap<String,ValueObject>();
    }
    this.values.put(key, val);
  }

  public Map<String,ValueObject> getValues() {
    return this.values;
  }

  public Arguments setValues(Map<String,ValueObject> values) {
    this.values = values;
    return this;
  }

  public void unsetValues() {
    this.values = null;
  }

  /** Returns true if field values is set (has been assigned a value) and false otherwise */
  public boolean isSetValues() {
    return this.values != null;
  }

  public void setValuesIsSet(boolean value) {
    if (!value) {
      this.values = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case VALUES:
      if (value == null) {
        unsetValues();
      } else {
        setValues((Map<String,ValueObject>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case VALUES:
      return getValues();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case VALUES:
      return isSetValues();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Arguments)
      return this.equals((Arguments)that);
    return false;
  }

  public boolean equals(Arguments that) {
    if (that == null)
      return false;

    boolean this_present_values = true && this.isSetValues();
    boolean that_present_values = true && that.isSetValues();
    if (this_present_values || that_present_values) {
      if (!(this_present_values && that_present_values))
        return false;
      if (!this.values.equals(that.values))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(Arguments other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Arguments typedOther = (Arguments)other;

    lastComparison = Boolean.valueOf(isSetValues()).compareTo(typedOther.isSetValues());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValues()) {
      lastComparison = org.apache.blur.thirdparty.thrift_0_9_0.TBaseHelper.compareTo(this.values, typedOther.values);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol iprot) throws org.apache.blur.thirdparty.thrift_0_9_0.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol oprot) throws org.apache.blur.thirdparty.thrift_0_9_0.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Arguments(");
    boolean first = true;

    sb.append("values:");
    if (this.values == null) {
      sb.append("null");
    } else {
      sb.append(this.values);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.blur.thirdparty.thrift_0_9_0.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.blur.thirdparty.thrift_0_9_0.protocol.TCompactProtocol(new org.apache.blur.thirdparty.thrift_0_9_0.transport.TIOStreamTransport(out)));
    } catch (org.apache.blur.thirdparty.thrift_0_9_0.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.blur.thirdparty.thrift_0_9_0.protocol.TCompactProtocol(new org.apache.blur.thirdparty.thrift_0_9_0.transport.TIOStreamTransport(in)));
    } catch (org.apache.blur.thirdparty.thrift_0_9_0.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ArgumentsStandardSchemeFactory implements SchemeFactory {
    public ArgumentsStandardScheme getScheme() {
      return new ArgumentsStandardScheme();
    }
  }

  private static class ArgumentsStandardScheme extends StandardScheme<Arguments> {

    public void read(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol iprot, Arguments struct) throws org.apache.blur.thirdparty.thrift_0_9_0.TException {
      org.apache.blur.thirdparty.thrift_0_9_0.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.blur.thirdparty.thrift_0_9_0.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // VALUES
            if (schemeField.type == org.apache.blur.thirdparty.thrift_0_9_0.protocol.TType.MAP) {
              {
                org.apache.blur.thirdparty.thrift_0_9_0.protocol.TMap _map260 = iprot.readMapBegin();
                struct.values = new HashMap<String,ValueObject>(2*_map260.size);
                for (int _i261 = 0; _i261 < _map260.size; ++_i261)
                {
                  String _key262; // required
                  ValueObject _val263; // required
                  _key262 = iprot.readString();
                  _val263 = new ValueObject();
                  _val263.read(iprot);
                  struct.values.put(_key262, _val263);
                }
                iprot.readMapEnd();
              }
              struct.setValuesIsSet(true);
            } else { 
              org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol oprot, Arguments struct) throws org.apache.blur.thirdparty.thrift_0_9_0.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.values != null) {
        oprot.writeFieldBegin(VALUES_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.blur.thirdparty.thrift_0_9_0.protocol.TMap(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TType.STRING, org.apache.blur.thirdparty.thrift_0_9_0.protocol.TType.STRUCT, struct.values.size()));
          for (Map.Entry<String, ValueObject> _iter264 : struct.values.entrySet())
          {
            oprot.writeString(_iter264.getKey());
            _iter264.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ArgumentsTupleSchemeFactory implements SchemeFactory {
    public ArgumentsTupleScheme getScheme() {
      return new ArgumentsTupleScheme();
    }
  }

  private static class ArgumentsTupleScheme extends TupleScheme<Arguments> {

    @Override
    public void write(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol prot, Arguments struct) throws org.apache.blur.thirdparty.thrift_0_9_0.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetValues()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetValues()) {
        {
          oprot.writeI32(struct.values.size());
          for (Map.Entry<String, ValueObject> _iter265 : struct.values.entrySet())
          {
            oprot.writeString(_iter265.getKey());
            _iter265.getValue().write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol prot, Arguments struct) throws org.apache.blur.thirdparty.thrift_0_9_0.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.blur.thirdparty.thrift_0_9_0.protocol.TMap _map266 = new org.apache.blur.thirdparty.thrift_0_9_0.protocol.TMap(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TType.STRING, org.apache.blur.thirdparty.thrift_0_9_0.protocol.TType.STRUCT, iprot.readI32());
          struct.values = new HashMap<String,ValueObject>(2*_map266.size);
          for (int _i267 = 0; _i267 < _map266.size; ++_i267)
          {
            String _key268; // required
            ValueObject _val269; // required
            _key268 = iprot.readString();
            _val269 = new ValueObject();
            _val269.read(iprot);
            struct.values.put(_key268, _val269);
          }
        }
        struct.setValuesIsSet(true);
      }
    }
  }

}

