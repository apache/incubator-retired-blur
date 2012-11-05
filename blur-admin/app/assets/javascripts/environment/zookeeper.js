var ZookeeperModel = Backbone.Model.extend({
  clusters: new ClusterCollection(),
  controllers: new ControllerCollection(),
  initialize: function(){
    this.view = new ZookeeperView({model: this, el: '#zookeeper'});
    // Whenever a property changes re-render the view
    this.on('change', function(){
      this.view.render();
    });
  },
  parse: function(response){
    this.clusters.update(response.clusters);
    this.controllers.update(response.blur_controllers);

    delete response.clusters
    delete response.blur_controllers

    this.set(response);
  },
  // Model streaming, fetches on every interval
  stream: function(interval){
    var _update = _.bind(function() {
      this.fetch({
        url: Routes.zookeeper_path(CurrentZookeeper, {format: 'json'})
      });
      window.setTimeout(_update, interval);
    }, this);
    _update();
  },
  // Destroys the zookeeper on the server side
  remove: function(){
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
  },
  header: function(){
    return this.get('name') + " - Zookeeper - " + this.translated_status();
  },
  quarum_failed: function(){
    var totalZookeeperNodes = this.get('url').split(',').length;
    var totalOnlineNodes = this.get('ensemble').length;
    if (totalOnlineNodes > 0 && totalOnlineNodes != totalZookeeperNodes){
      return true;
    }
    return false;
  },
  // The translated status
  translated_status: function(){
    switch(this.get('status'))
    {
      case 0:
        if (this.quarum_failed()){
          return "Quarum Failure"
        }
        return "Offline"
      case 1:
        return "Online"
      case 2:
        return "Ensemble Warning"
    }
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
  }
});

var ZookeeperView = Backbone.View.extend({
  events: {
    "click .destroy-zk" : "destroy_zookeeper"
  },
  template: JST['templates/environment/zookeeper'],
  render: function(){
    this.$el.html(this.template({zookeeper: this.model}));
    this.$el.removeClass('btn-danger btn-success btn-warning');
    this.$el.addClass(this.model.translated_class());
    this.$('i').tooltip();
    return this;
  },
  destroy_zookeeper: function(){
    Confirm_Delete({
      message: "forget this zookeeper",
      confirmed_action: this.model.remove
    });
  }
});