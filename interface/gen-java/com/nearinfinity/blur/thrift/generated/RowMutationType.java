/**
 * Autogenerated by Thrift Compiler (0.7.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.nearinfinity.blur.thrift.generated;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

/**
 * Specifies the type of Row mutation that should occur during a mutation of a given Row.<br/><br/>
 * DELETE_ROW - Indicates that the entire Row is to be deleted.  No changes are made if the specified row does not exist.<br/><br/>
 * REPLACE_ROW - Indicates that the entire Row is to be deleted, and then a new Row with the same id is to be added.  If the specified row does not exist, the new row will still be created.<br/><br/>
 * UPDATE_ROW - Indicates that mutations of the underlying Records will be processed individually.  Mutation will result in a BlurException if the specified row does not exist.<br/>
 */
public enum RowMutationType implements org.apache.thrift.TEnum {
  DELETE_ROW(0),
  REPLACE_ROW(1),
  UPDATE_ROW(2);

  private final int value;

  private RowMutationType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static RowMutationType findByValue(int value) { 
    switch (value) {
      case 0:
        return DELETE_ROW;
      case 1:
        return REPLACE_ROW;
      case 2:
        return UPDATE_ROW;
      default:
        return null;
    }
  }
}
