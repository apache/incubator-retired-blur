//= require jquery.dynatree
//= require bootstrap-tooltip
//= require bootstrap-popover
//= require_tree .

$(document).ready(function() {
  // Dynatree Setup
  $.ui.dynatree.nodedatadefaults["icon"] = false;

  // Create the cluster collection and start the stream
  window.clusters = new ClusterCollection();
  clusters.stream({interval: 10000, update: true});
});