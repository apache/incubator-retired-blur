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
package org.apache.blur.utils;

import static org.apache.blur.metrics.MetricsConstants.GC_TIMES;
import static org.apache.blur.metrics.MetricsConstants.JVM;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

import com.sun.management.GcInfo;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class GCWatcherJdk7 {

  private static final Log LOG = LogFactory.getLog(GCWatcherJdk7.class);
  private static final String GET_LAST_GC_INFO = "getLastGcInfo";
  private static final long _1_SECOND = TimeUnit.SECONDS.toMillis(1);
  private static GCWatcherJdk7 _instance;

  private final MemoryMXBean _memoryMXBean;
  private final double _ratio;
  private final List<GCAction> _actions = new ArrayList<GCAction>();
  private final Timer _gcTimes;

  private GCWatcherJdk7(double ratio) {
    _memoryMXBean = ManagementFactory.getMemoryMXBean();

    MetricName gcTimesName = new MetricName(ORG_APACHE_BLUR, JVM, GC_TIMES);
    _gcTimes = Metrics.newTimer(gcTimesName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean bean : garbageCollectorMXBeans) {
      NotificationListener listener = new NotificationListener() {
        @Override
        public void handleNotification(Notification notification, Object bean) {
          GarbageCollectorMXBean garbageCollectorMXBean = (GarbageCollectorMXBean) bean;
          GcInfo gcInfo = getGcInfo(garbageCollectorMXBean);
          if (gcInfo != null) {
            long startTime = gcInfo.getStartTime();
            long endTime = gcInfo.getEndTime();
            Map<String, MemoryUsage> usageBeforeGc = gcInfo.getMemoryUsageBeforeGc();
            Map<String, MemoryUsage> usageAfterGc = gcInfo.getMemoryUsageAfterGc();
            long usedBefore = getTotal(usageBeforeGc);
            long usedAfter = getTotal(usageAfterGc);
            long totalTime = endTime - startTime;
            long totalSize = usedBefore - usedAfter;
            if (totalTime >= _1_SECOND) {
              LOG.info("GC event totalTime spent in GC [{0} ms] collected [{1} bytes]", totalTime, totalSize);
            }
            _gcTimes.update(totalTime, TimeUnit.MILLISECONDS);

            MemoryUsage heapMemoryUsage = _memoryMXBean.getHeapMemoryUsage();
            long max = heapMemoryUsage.getMax();
            long used = heapMemoryUsage.getUsed();
            long upperLimit = (long) (max * _ratio);
            if (used > upperLimit) {
              LOG.error(
                  "----- WARNING !!!! - Heap used [{0}] over limit of [{1}], taking action to avoid an OOM error.",
                  used, upperLimit);
              takeAction();
            }
          } else {
            LOG.warn("GCInfo was null.  Cannot report on GC activity.");
          }
        }

        private void takeAction() {
          synchronized (_actions) {
            for (GCAction action : _actions) {
              try {
                action.takeAction();
              } catch (Exception e) {
                LOG.error("Unknown error while trying to take action against an OOM [{0}]", e, action);
              }
            }
          }
        }

        private long getTotal(Map<String, MemoryUsage> memoryUsage) {
          long used = 0;
          for (Entry<String, MemoryUsage> e : memoryUsage.entrySet()) {
            used += e.getValue().getUsed();
          }
          return used;
        }
      };

      NotificationFilter filter = new NotificationFilter() {
        private static final long serialVersionUID = 2971450191223596323L;

        @Override
        public boolean isNotificationEnabled(Notification notification) {
          return true;
        }
      };

      Method method;
      try {
        method = bean.getClass().getMethod("addNotificationListener",
            new Class[] { NotificationListener.class, NotificationFilter.class, Object.class });
        method.setAccessible(true);
        method.invoke(bean, new Object[] { listener, filter, bean });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }
    _ratio = ratio;
    LOG.info("GCWatcherJdk7 was setup.");
  }

  public static synchronized void init(double ratio) {
    if (_instance == null) {
      try {
        _instance = new GCWatcherJdk7(ratio);
      } catch (Exception e) {
        LOG.error("GCWatcher had error initializing", e);
      }
    } else {
      LOG.warn("GCWatcher has already been initialized");
    }
  }

  public static void registerAction(GCAction action) {
    GCWatcherJdk7 instance = instance();
    if (instance != null) {
      synchronized (instance._actions) {
        instance._actions.add(action);
      }
    }
  }

  public static void shutdown() {

  }

  private static GCWatcherJdk7 instance() {
    return _instance;
  }

  private GcInfo getGcInfo(GarbageCollectorMXBean bean) {
    try {
      Method method = bean.getClass().getMethod(GET_LAST_GC_INFO, new Class[] {});
      method.setAccessible(true);
      return (GcInfo) method.invoke(bean, new Object[] {});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
