var ZookeeperModel = Backbone.Model.extend({
  initialize: function(){
    this.view = new ZookeeperView({model: this});
    // Whenever a property changes re-render the view
    this.on('change', function(){
      this.view.render();
    });
  },
  parse: function(response){
    // Build the collections and add them to the zookeeper
    if(this.cluster_collection && this.controller_collection){
      this.cluster_collection.update(response.clusters);
      this.controller_collection.update(response.blur_controllers);
    } else {
      this.cluster_collection = new ClusterCollection(response.clusters);
      this.controller_collection = new ControllerCollection(response.blur_controllers);
    }

    //remove the collections
    delete response.clusters
    delete response.controllers

    this.set(response);
  },
  // Model streaming, fetches on every interval
  stream: function(interval){
    var _update = _.bind(function() {
      this.fetch({url: Routes.zookeeper_path(CurrentZookeeper, {format: 'json'})});
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
    switch(this.get('status'))
    {
      case 0:
        return "btn-danger"
      case 1:
        return "btn-success"
      case 2:
        return "btn-warning"
    }
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
    this.$el.find('#controllers').append(this.model.controller_collection.view.render().$el)
    this.$el.find('#clusters').append(this.model.cluster_collection.view.render().$el)
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