package org.apache.blur.manager.stats;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class WeightedAvg {

  private final int _maxSize;
  private final long[] _values;
  private int _numberOfAdds;
  private int _currentPosition;
  private long _totalValue = 0;

  public WeightedAvg(int maxSize) {
    _maxSize = maxSize;
    _values = new long[_maxSize];
    _numberOfAdds = 0;
    _currentPosition = 0;
  }

  public void add(long value) {
    if (_currentPosition >= _maxSize) {
      _currentPosition = 0;
    }
    long currentValue = _values[_currentPosition];
    _values[_currentPosition] = value;
    _totalValue += value - currentValue;
    _numberOfAdds++;
    _currentPosition++;
  }

  public double getAvg() {
    if (_totalValue == 0) {
      return 0;
    }
    return (double) _totalValue / (double) Math.min(_numberOfAdds, _maxSize);
  }

  public int getMaxSize() {
    return _maxSize;
  }

  public long[] getValues() {
    return _values;
  }

  public int getNumberOfAdds() {
    return _numberOfAdds;
  }

  public int getCurrentPosition() {
    return _currentPosition;
  }

  public long getTotalValue() {
    return _totalValue;
  }

}
