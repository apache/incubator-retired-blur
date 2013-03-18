package org.apache.blur.metrics;

import java.util.concurrent.atomic.AtomicLong;

import com.yammer.metrics.core.Gauge;

public class AtomicLongGauge extends Gauge<Long> {

  private final AtomicLong at;

  public AtomicLongGauge(AtomicLong at) {
    this.at = at;
  }

  @Override
  public Long value() {
    return at.get();
  }

  public static Gauge<Long> wrap(AtomicLong at) {
    return new AtomicLongGauge(at);
  }

}
