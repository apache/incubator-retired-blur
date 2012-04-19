//= require jquery.dynatree
//= require bootstrap-tooltip
//= require bootstrap-popover

// <-------------- Models -------------->

var Cluster = Backbone.Model.extend({
  defaults: {},
  initialize: function(){
    this.view = new ClusterView({model: this, id: 'cluster_' + this.id});
    this.build_child_tables();
    this.on('change:blur_tables', function(){
      this.build_child_tables();
    });
    this.on('change:safe_mode', function(){
      $('li#cluster_tab_' + this.get('id') + ' .safemode-icon').toggle();
    });
    this.on('table_has_been_queried', function(){
      var tables_queried = this.get('tables').where({queried_recently: true}).length
      if (tables_queried > 0) {
        $('li#cluster_tab_' + this.get('id') + ' .queries-running-icon').show();
      }
      $('li#cluster_tab_' + this.get('id') + ' .queries-running-icon').hide();
    });
  },
  build_child_tables: function(){
    var self = this;
    this.set({tables: new TableCollection(this.get('blur_tables'), {cluster: this})}, {silent: true});
  }
});

var Table = Backbone.Model.extend({
  defaults: {
    'checked' : false,
  },
  state_lookup : ['deleted', 'deleting', 'disabled', 'disabling', 'active', 'enabling'],
  table_lookup : ['deleted', 'disabled', 'disabled', 'active', 'active', 'disabled'],
  colspan_lookup : {'active': 5, 'disabled': 3, 'deleted': 1},
  initialize: function(){
    this.view = new TableView({model: this});
    this.set({state: this.state_lookup[this.get('status')]});
    this.on('change:status', function(){
      this.set({state: this.state_lookup[this.get('status')]});
    });
    this.on('change:queried_recently', function(){
      this.collection.cluster.trigger('table_has_been_queried');
    });

  },
  host_shard_string: function(){
    var server = JSON.parse(this.get('server'));
    if (server) {
      var count, hosts = 0;
      for (var key in server) {
        hosts++;
        count += server[key].length;
      }
      return hosts + ' | ' + count;
    } else {
      return "Schema Information is Currently Unavailable";
    }
  },
  capitalize_first: function(word){
    return word.charAt(0).toUpperCase() + word.slice(1);
  }
});

// <-------------- Collections -------------->

var ClusterCollection = Backbone.Collection.extend({
  model: Cluster,
  url: Routes.zookeeper_blur_tables_path(CurrentZookeeper, {format: 'json'}),
  initialize: function(models, options){
    this.on('add', function(collection){
      var container = $('#tables-wrapper');
      var renderedView = $(collection.view.render().el);
      container.children('div').length === 0 ? container.html(renderedView.addClass('active')) : container.append(renderedView);
    });
  }
});

var TableCollection = Backbone.Collection.extend({
  model: Table,
  initialize: function(models, options){
    this.cluster = options.cluster;
  }
});

// <-------------- Views -------------->

var ClusterView = Backbone.View.extend({
  className: 'tab-pane cluster',
  template: JST['templates/blur_table/cluster_view'],
  events: {},
  render: function(){
    $(this.el).html(this.template({cluster: this.model}));
    return this;
  }
});

var TableView = Backbone.View.extend({
  class: 'tab-pane cluster',
  events: {},
  render: function(){},
});

$(document).ready(function() {
  window.clusters = new ClusterCollection();
  clusters.stream({interval: 10000, update: true});
});