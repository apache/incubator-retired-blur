package com.nearinfinity.blur.manager.writer;

import java.io.IOException;
import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class BlurIndexRefresher extends TimerTask {

  private static final Log LOG = LogFactory.getLog(BlurIndexRefresher.class);

  private Timer _timer;
  private long _period = TimeUnit.MINUTES.toMillis(1);
  private long _delay = _period;
  private Collection<BlurIndex> _indexes = new LinkedBlockingQueue<BlurIndex>();

  public void register(BlurIndex blurIndex) {
    _indexes.add(blurIndex);
  }

  public void unregister(BlurIndex blurIndex) {
    _indexes.remove(blurIndex);
  }

  public void close() {
    _timer.purge();
    _timer.cancel();
  }

  public void init() {
    LOG.info("init - start");
    _timer = new Timer("IndexReader-Refresher", true);
    _timer.schedule(this, _delay, _period);
    LOG.info("init - complete");
  }

  @Override
  public void run() {
    for (BlurIndex index : _indexes) {
      try {
        index.refresh();
      } catch (IOException e) {
        LOG.error("Unknown error while refreshing index of writer [{0}]", e, index);
      }
    }
  }

  public void setPeriod(long period) {
    _period = period;
  }

  public void setDelay(long delay) {
    _delay = delay;
  }

}
