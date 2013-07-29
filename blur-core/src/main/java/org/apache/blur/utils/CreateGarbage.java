package org.apache.blur.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.utils.GCWatcher.Action;

public class CreateGarbage {

  public static void main(String[] args) throws InterruptedException {
    final Map<String, String> map = new ConcurrentHashMap<String, String>();
    Action action = new Action() {
      @Override
      public void takeAction() throws Exception {
        map.clear();
      }
    };
    GCWatcher.init(0.75);
    GCWatcher.registerAction(action);
    while (true) {

      int count = 0;
      int max = 100000;
      for (long i = 0; i < 100000000; i++) {
        if (count >= max) {
          Thread.sleep(250);
          count = 0;
        }
        map.put(Long.toString(i), Long.toString(i));
        count++;
      }
      System.out.println(map.size());
      map.clear();
    }
  }

}
