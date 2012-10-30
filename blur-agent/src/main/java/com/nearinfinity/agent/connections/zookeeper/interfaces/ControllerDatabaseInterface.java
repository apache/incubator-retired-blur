package com.nearinfinity.agent.connections.zookeeper.interfaces;

import java.util.List;

public interface ControllerDatabaseInterface {
  
  int markOfflineControllers(List<String> onlineControllers, int zookeeperId);

  void updateOnlineController(String controller, int zookeeperId, String blurVersion);
  
  List<String> getRecentOfflineControllerNames(int amount);
}
