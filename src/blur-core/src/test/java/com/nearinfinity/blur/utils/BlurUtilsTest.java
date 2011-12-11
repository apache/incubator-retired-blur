package com.nearinfinity.blur.utils;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.*;

public class BlurUtilsTest {
  
  @Test
  public void testHumanizeTime1() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(42) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 42 minutes 37 seconds", humanizeTime);
  }
  
  @Test
  public void testHumanizeTime2() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(42) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("42 minutes 37 seconds", humanizeTime);
  }
  
  @Test
  public void testHumanizeTime3() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 0 minutes 37 seconds", humanizeTime);
  }
  
  @Test
  public void testHumanizeTime4() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(0) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 0 minutes 0 seconds", humanizeTime);
  }
  
  @Test
  public void testHumanizeTime5() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("37 seconds", humanizeTime);
  }
  
  @Test
  public void testHumanizeTime6() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(0) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("0 seconds", humanizeTime);
  }
  
  

}
