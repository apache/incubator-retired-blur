var Zookeeper = Backbone.Model.extend({
  initialize: function(){
    this.view = new ZookeeperView({model: this});
    this.on('change', function(){
      this.view.render();
    });
  },
  consistent_controller_versions: function(){
    return this.get('controller_version') === 1;
  },
  consistent_shard_versions: function(){
    return this.get('shard_version') === 1;
  },
  online_controller_nodes: function(){
    return this.get('controller_total') - this.get('controller_offline_node');
  },
  online_shard_nodes: function(){
    return this.get('shard_total') - this.get('shard_offline_node');
  },
  controller_progress_width: function(){
    return Math.round((this.online_controller_nodes() / this.get('controller_total')) * 100)
  },
  shard_progress_width: function(){
    return Math.round((this.online_shard_nodes() / this.get('shard_total')) * 100)
  },
  status: function(){
    if(this.get('status') === 0) return 'Offline';
    return 'Online';
  }
});

var ZookeeperCollection = Backbone.StreamCollection.extend({
  model: Zookeeper,
  url: Routes.dashboard_zookeepers_path({format: 'json'}),
  initialize: function(models, options){
    this.on('add', function(zookeeper){
      $('#zookeepers').append(zookeeper.view.render().el);
    });
    this.on('remove', function(zookeeper){
      zookeeper.view.remove();
      zookeeper.destroy();
    });
  }
});

var ZookeeperView = Backbone.View.extend({
  className: 'zookeeper_info',
  events: {
    'click .zookeeper-table' : 'navigate_to_zookeeper',
    'click .warning' : 'show_long_running'
  },
  template: JST['templates/dashboard/zookeeper'],
  render: function(){
    this.$el.html(this.template({zookeeper: this.model}));
    if(this.model.get('status') === 0){
      this.$el.find('.zookeeper-title').addClass('btn-danger');
      this.$el.addClass('offline');
    } else {
      this.$el.addClass('online');
    }
    return this;
  },
  navigate_to_zookeeper: function(){
    window.location = Routes.zookeeper_path(this.model.get('id'));
  },
  show_long_running: function(){
    $.ajax({
      type: 'GET',
      url: Routes.long_running_queries_zookeeper_path(this.model.get('id')),
      success: function(data){
        $().popup({
          title: "Long Running Queries",
          titleClass: 'title',
          body: new LongRunningView().render(data).el
        });
      }
    })
    return false;
  }
});