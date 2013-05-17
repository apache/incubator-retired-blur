var ZookeeperModel = Backbone.Model.extend({
  clusters: new ClusterCollection(),
  controllers: new ControllerCollection(),
  initialize: function(){
    this.view = new ZookeeperView({model: this, el: '#zookeeper'});
    // Whenever a property changes re-render the view
    this.on('change', function(){
      this.view.render();
    });
    this.initial_load = true;
  },
  url: function(){
    return '/zookeepers/' + this.get('id') + '.json';
  },
  parse: function(response){
    if (this.initial_load){
      if (response.clusters.length <= 0){
        this.clusters.view.$el.find('.no_children').show();
      }
      if (response.blur_controllers.length <= 0){
        this.blur_controllers.view.$el.find('.no_children').show();
      }
      this.initial_load = false;
    }
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
    if(this.get('zookeeper_status') == 0){
      this.destroy({
        success: function(){
          window.location = window.location.origin;
        },
        error: function(){
          Notification("Failed to forget the Zookeeper", false);
        }
      });
    } else {
      Notification("Cannot forget a Zookeeper that is online!", false);
    }
  },
  header: function(){
    return this.get('name') + " - Zookeeper - " + this.translated_status();
  },
  quarum_failed: function(){
    return this.get('zookeeper_status') == 3
  },
  offline_nodes: function(){
    var allNodes = this.get('url').split(',')
    var online = this.get('ensemble');
    var offline = [];
    for (var i = 0; i < allNodes.length; i++){
      var node = allNodes[i];
      if (online.indexOf(node) < 0){
        offline.push(node)
      }
    }
    return offline;
  },
  // The translated status
  translated_status: function(){
    switch(this.get('zookeeper_status'))
    {
      case 0:
        return "Offline"
      case 1:
        return "Online"
      case 2:
        return "Ensemble Warning"
      case 3:
        return "Quorum Failure"
    }
  },
  // Determines the class for the state of the zookeeper
  translated_class: function(){
    switch(this.get('zookeeper_status'))
    {
      case 0:
        return "btn-danger"
      case 1:
        return "btn-success"
      case 2:
        return "btn-warning"
      case 3:
        return "btn-danger"
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
    this.$('span.states').tooltip({placement: 'bottom'});
    return this;
  },
  destroy_zookeeper: function(){
    Confirm_Delete({
      message: "forget this zookeeper",
      confirmed_action: _.bind(this.model.remove, this.model)
    });
  }
});