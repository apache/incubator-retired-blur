var ZookeeperModel = Backbone.Model.extend({
  initialize: function(){
    this.view = new ZookeeperView({model: this});
    // Build/rebuild the one to many relationship on cluster change
    this.on('change:clusters', function(){
      var clusters = this.cluster_collection || new ClusterCollection();
      clusters.update(this.get('clusters'));
      this.cluster_collection = clusters;
    });
    // Build/rebuild the one to many relationship on controller change
    this.on('change:controllers', function(){
      var controllers = this.controller_collection || new ControllerCollection();
      controllers.update(this.get('controllers'));
      this.controller_collection = controllers;
    });
    // Whenever a property changes re-render the view
    this.on('change', function(){
      this.view.render();
    });
  },
  // Model streaming, fetches on every interval
  stream: function(interval){
    var _update = _.bind(function() {
      this.fetch({url: Routes.zookeeper_path(CurrentZookeeper)});
      window.setTimeout(_update, interval);
    }, this);
    _update();
  },
  // Translated zookeeper header
  header: function(){
    return this.get('name') + " - Zookeeper - " + this.translated_status();
  },
  // The translated status
  translated_status: function(){
    return this.get('status') ? "Online" : "Offline";
  },
  // Determines the class for the state of the zookeeper
  translated_class: function(){
    return this.get('status') ? "btn-success" : "btn-danger";
  },
  // Destroys the zookeeper on the server side
  remove_zookeeper: function(){
    if(this.get('status') == 0){
      this.destroy({
        success: function(){
          window.location = window.location.origin;
        }, 
        error: function(){
          Notification("Failed to forget the Zookeeper", false);
        }
      });
    }
  }
});

var ZookeeperView = Backbone.View.extend({
  id: 'zookeeper_wrapper',
  events: {
    "click .destroy-zk" : "destroy_zookeeper"
  },
  template: JST['templates/environment/zookeeper'],
  render: function(){
    this.$el.html(this.template({zookeeper: this.model}));
    this.$el.find('#controllers').append(this.model.controller_collection.view.$el)
    this.$el.find('#clusters').append(this.model.cluster_collection.view.$el)
    $('#bd').html(this.$el);
    this.$el.find('i').tooltip();
    return this;
  },
  destroy_zookeeper: function(){
    Confirm_Delete({
      message: "forget this zookeeper",
      confirmed_action: this.model.remove_zookeeper
    });
  }
});