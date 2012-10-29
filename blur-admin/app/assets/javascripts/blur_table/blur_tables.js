//= require jquery.dynatree
//= require bootstrap-tooltip
//= require bootstrap-popover
//= require sorttable
//= require_tree .

$(document).ready(function() {
  // Dynatree Setup
  $.ui.dynatree.nodedatadefaults["icon"] = false;

  // Create the cluster collection and start the stream
  new ClusterCollection().stream({interval: 5000, update: true});
});
