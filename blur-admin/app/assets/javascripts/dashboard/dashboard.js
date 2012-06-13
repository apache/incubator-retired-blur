//= require_tree

$(document).ready(function() {
  // Create the hdfs and blur instances and start streaming
  new HdfsCollection().stream({interval: 500000, update: true});
  new ZookeeperCollection().stream({interval: 500000, update: true});
});
