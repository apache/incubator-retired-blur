var ZookeeperModel = Backbone.Model.extend({
  initialize: function(){
    this.view = new ZookeeperView({model: this});
    this.on('change:clusters', function(){
      var clusters = this.get('cluster_collection') || new ClusterCollection();
      clusters.update(this.get('clusters'));
      this.cluster_collection = clusters;
    });
    this.on('change:controllers', function(){
      var controllers = this.get('controller_collection') || new ControllerCollection();
      controllers.update(this.get('controllers'));
      this.controller_collection = controllers;
    });
    this.on('change', function(){
      this.view.render();
    });
  },
  stream: function(interval){
    var _update = _.bind(function() {
      this.fetch({url: Routes.zookeeper_path(CurrentZookeeper)});
      window.setTimeout(_update, interval);
    }, this);
    _update();
  },
  header: function(){
    return this.get('name') + " - Zookeeper - " + this.translated_status();
  },
  translated_status: function(){
    return this.get('status') ? "Online" : "Offline";
  },
  translated_class: function(){
    return this.get('status') ? "btn-success" : "btn-danger";
  }
});

var ZookeeperView = Backbone.View.extend({
  id: 'zookeeper_wrapper',
  events: {
  },
  template: JST['templates/environment/zookeeper'],
  render: function(){
    this.$el.html(this.template({zookeeper: this.model}));
    this.$el.find('#controllers').append(this.model.controller_collection.view.$el)
    this.$el.find('#clusters').append(this.model.cluster_collection.view.$el)
    $('#bd').html(this.$el);
    this.$el.find('i').tooltip();
    return this;
  }
});