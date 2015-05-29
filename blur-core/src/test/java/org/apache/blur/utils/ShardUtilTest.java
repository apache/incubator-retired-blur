package org.apache.blur.utils;

import static org.junit.Assert.*;

import org.junit.Test;

public class ShardUtilTest {

  @Test
  public void nullFamilyAndColumnNamesShouldFail() {
    assertFalse("Null names should fail.", ShardUtil.validate(null));
  }

}
