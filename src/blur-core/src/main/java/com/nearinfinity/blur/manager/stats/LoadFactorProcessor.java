package com.nearinfinity.blur.manager.stats;

import java.util.concurrent.TimeUnit;

public class LoadFactorProcessor {

  private Sampler _sampler;
  private WeightedAvg _one;
  private WeightedAvg _five;
  private WeightedAvg _fifteen;

  public LoadFactorProcessor(Sampler sampler) {
    _sampler = sampler;
    _one = new WeightedAvg((int) TimeUnit.MINUTES.toSeconds(1));
    _five = new WeightedAvg((int) TimeUnit.MINUTES.toSeconds(5));
    _fifteen = new WeightedAvg((int) TimeUnit.MINUTES.toSeconds(15));
  }

  public void sample() {
    long sample = _sampler.sample();
    _one.add(sample);
    _five.add(sample);
    _fifteen.add(sample);
  }

  public double oneMinuteLoadFactor() {
    return _one.getAvg();
  }

  public double fiveMinuteLoadFactor() {
    return _five.getAvg();
  }

  public double fifteenMinuteLoadFactor() {
    return _fifteen.getAvg();
  }

}
