package com.nearinfinity.blur.manager.stats;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class WeightedAvgTest {

  @Test
  public void testAvg() {
    int maxSize = 10;
    WeightedAvg weightedAvg = new WeightedAvg(maxSize);
    double total = 0;
    for (int i = 1; i <= maxSize; i++) {
      weightedAvg.add(i);
      total += i;
      assertTrue(Double.compare(total / i, weightedAvg.getAvg()) == 0);
    }
  }

}
