package com.nearinfinity.blur.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BlurMutate implements Writable {

  public enum MUTATE_TYPE {
    ADD(0), UPDATE(1), DELETE(2);
    private int _value;

    private MUTATE_TYPE(int value) {
      _value = value;
    }

    public int getValue() {
      return _value;
    }

    public MUTATE_TYPE find(int value) {
      switch (value) {
      case 0:
        return ADD;
      case 1:
        return UPDATE;
      case 2:
        return DELETE;
      default:
        throw new RuntimeException("Value [" + value + "] not found.");
      }
    }
  }

  private MUTATE_TYPE _mutateType = MUTATE_TYPE.UPDATE;
  private BlurRecord _record = new BlurRecord();

  public BlurRecord getRecord() {
    return _record;
  }

  public void setRecord(BlurRecord record) {
    _record = record;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    IOUtil.writeVInt(out, _mutateType.getValue());
    _record.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    _mutateType.find(IOUtil.readVInt(in));
    _record.readFields(in);
  }

  public MUTATE_TYPE getMutateType() {
    return _mutateType;
  }

  public void setMutateType(MUTATE_TYPE mutateType) {
    _mutateType = mutateType;
  }

}
