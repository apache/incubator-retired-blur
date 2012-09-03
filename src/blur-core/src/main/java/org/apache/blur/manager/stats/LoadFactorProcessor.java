package org.apache.blur.manager.stats;

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
import java.util.concurrent.TimeUnit;

public class LoadFactorProcessor {

  private final Sampler _sampler;
  private final WeightedAvg _one;
  private final WeightedAvg _five;
  private final WeightedAvg _fifteen;

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

  public Sampler getSampler() {
    return _sampler;
  }

  public WeightedAvg getOne() {
    return _one;
  }

  public WeightedAvg getFive() {
    return _five;
  }

  public WeightedAvg getFifteen() {
    return _fifteen;
  }

}
