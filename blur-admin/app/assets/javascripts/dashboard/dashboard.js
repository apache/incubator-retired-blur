//= require_tree .

$(document).ready(function() {
  // Create the hdfs and blur instances and start streaming
  new HdfsCollection().stream({interval: 5000, update: true});
  new ZookeeperCollection().stream({interval: 5000, update: true});
});