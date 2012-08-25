package com.nearinfinity.blur.manager.stats;


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
