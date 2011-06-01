package com.nearinfinity.blur.manager.stats;

import java.util.concurrent.atomic.AtomicLong;

public class WeightedAvg {
    
    private int _maxSize;
    private long[] _values;
    private int _numberOfAdds;
    private int _currentPosition;
    private AtomicLong _totalValue = new AtomicLong(0);
    
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
        _totalValue.addAndGet(value - currentValue);
        _numberOfAdds++;
        _currentPosition++;
    }
    
    public double getAvg() {
        long v = _totalValue.get();
        if (v == 0) {
            return 0;
        }
        return (double) v / (double) Math.min(_numberOfAdds,_maxSize);
    }

}
