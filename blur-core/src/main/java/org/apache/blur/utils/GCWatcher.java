package org.apache.blur.utils;

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
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

public class GCWatcher extends TimerTask {

  private static final Log LOG = LogFactory.getLog(GCWatcher.class);
  private static final String GET_LAST_GC_INFO = "getLastGcInfo";
  private static final String CONCURRENT_MARK_SWEEP = "ConcurrentMarkSweep";
  private static final String CMS_OLD_GEN = "CMS Old Gen";

  private final Timer _timer;
  private final long _period = TimeUnit.MILLISECONDS.toMillis(25);
  private GarbageCollectorMXBean _bean;
  private final double _ratio;
  private Method _method;
  private GcInfo _gcInfo;
  private long _lastIndex;
  private List<Action> _actions = new ArrayList<Action>();
  private static GCWatcher _instance;

  public interface Action {
    void takeAction() throws Exception;
  }

  private GCWatcher(double ratio) {
    List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean bean : garbageCollectorMXBeans) {
      String name = bean.getName();
      if (name.equals(CONCURRENT_MARK_SWEEP)) {
        _bean = bean;
        try {
          _method = _bean.getClass().getDeclaredMethod(GET_LAST_GC_INFO, new Class[] {});
        } catch (SecurityException e) {
          throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
          throw new RuntimeException(e);
        }
        _method.setAccessible(true);
      }
    }
    _ratio = ratio;
    if (_bean != null) {
      _timer = new Timer("gc-watch", true);
      _timer.schedule(this, _period, _period);
      LOG.info("GCWatcher was setup.");
    } else {
      _timer = null;
      LOG.warn("GCWatcher was NOT setup.");
    }
  }

  public static void registerAction(Action action) {
    GCWatcher instance = instance();
    if (instance != null) {
      synchronized (instance._actions) {
        instance._actions.add(action);
      }
    }
  }

  public static synchronized void shutdown() {
    GCWatcher instance = instance();
    if (instance != null) {
      if (instance._timer != null) {
        instance._timer.purge();
        instance._timer.cancel();
      }
    }
  }

  @Override
  public void run() {
    try {
      Object gcInfo = getGcInfo(_bean);
      if (gcInfo != null) {
        if (_gcInfo == null) {
          _gcInfo = new GcInfo(gcInfo);
        } else {
          _gcInfo.setGcInfo(gcInfo);
        }
        if (_lastIndex == _gcInfo.getIndex()) {
          return;
        }
        long startTime = _gcInfo.getStartTime();
        long endTime = _gcInfo.getEndTime();
        Map<String, MemoryUsage> usageAfterGc = _gcInfo.getUsageAfterGc();
        MemoryUsage after = usageAfterGc.get(CMS_OLD_GEN);
        long used = after.getUsed();
        long max = after.getMax();

        long totalTime = endTime - startTime;
        LOG.info("totalTime spent in GC [{0}] collected [{1}]", totalTime, (max - used));
        long upperLimit = (long) (max * _ratio);
        if (used > upperLimit) {
          LOG.error("----- WARNING !!!! - Heap used [{0}] over limit of [{1}], taking action to avoid an OOM error.", used,
              upperLimit);
          synchronized (_actions) {
            for (Action action : _actions) {
              try {
                action.takeAction();
              } catch (Exception e) {
                LOG.error("Unknown error while trying to take action against an OOM [{0}]", e, action);
              }
            }
          }
        }
        _lastIndex = _gcInfo.getIndex();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private Object getGcInfo(GarbageCollectorMXBean bean) throws Exception {
    return _method.invoke(bean, new Object[] {});
  }

  static class GcInfo {

    private Object _gcInfo;
    private final Field _startTimeField;
    private final Field _endTimeField;
    private final Field _usageAfterGcField;
    private final Field _usageBeforeGcField;
    private final Field _indexField;

    GcInfo(Object o) throws SecurityException, NoSuchFieldException {
      Class<? extends Object> clazz = o.getClass();
      _startTimeField = setup(clazz.getDeclaredField("startTime"));
      _endTimeField = setup(clazz.getDeclaredField("endTime"));
      _usageAfterGcField = setup(clazz.getDeclaredField("usageAfterGc"));
      _usageBeforeGcField = setup(clazz.getDeclaredField("usageBeforeGc"));
      _indexField = setup(clazz.getDeclaredField("index"));
      setGcInfo(o);
    }

    void setGcInfo(Object o) {
      _gcInfo = o;
    }

    private Field setup(Field field) {
      field.setAccessible(true);
      return field;
    }

    long getStartTime() {
      try {
        return _startTimeField.getLong(_gcInfo);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    long getEndTime() {
      try {
        return _endTimeField.getLong(_gcInfo);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    long getIndex() {
      try {
        return _indexField.getLong(_gcInfo);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings("unchecked")
    Map<String, MemoryUsage> getUsageAfterGc() {
      try {
        return (Map<String, MemoryUsage>) _usageAfterGcField.get(_gcInfo);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings("unchecked")
    Map<String, MemoryUsage> getUsageBeforeGc() {
      try {
        return (Map<String, MemoryUsage>) _usageBeforeGcField.get(_gcInfo);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  public synchronized static void init(double ratio) {
    if (_instance == null) {
      try {
        _instance = new GCWatcher(ratio);
      } catch (Exception e) {
        LOG.error("GCWatcher had error initializing", e);
      }
    } else {
      LOG.warn("GCWatcher has already been initialized");
    }
  }

  private static GCWatcher instance() {
    return _instance;
  }

}
