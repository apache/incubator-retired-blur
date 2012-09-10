package com.nearinfinity.agent.types;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("serial")
public class TableMap extends ConcurrentHashMap<String, Map<String, Object>> {
  /* Prevents TableMap declaration */
  private TableMap(){}
  
  private static class LazyTableLoader {
    /* Static TableMap instance (singleton pattern also lazyloaded) */ 
    public static final TableMap INSTANCE = new TableMap();
  }
  
  /**
   * @return Returns the <code>TableMap</code> singleton instance
   */
  public static TableMap getInstance() {
    return LazyTableLoader.INSTANCE;
  }
}

