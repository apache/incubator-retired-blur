//= require jquery.dynatree
//= require bootstrap-tooltip
//= require bootstrap-popover
//= require ./cluster
//= require ./table

$(document).ready(function() {
  var setup_filter_tree = function(selector) {
    return selector.dynatree();
  };
  $.ui.dynatree.nodedatadefaults["icon"] = false;

  window.clusters = new ClusterCollection();
  clusters.stream({interval: 10000, update: true});
});