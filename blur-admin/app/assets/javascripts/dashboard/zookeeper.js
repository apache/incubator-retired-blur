//= require flot/flot
//= require flot/jquery.flot.pie.min.js

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
    'click .zookeeper-body' : 'navigate_to_zookeeper',
    'click .warning' : 'show_long_running'
  },
  template: JST['templates/dashboard/zookeeper'],
  render: function(){
    this.$el.html(this.template({zookeeper: this.model}));
    if (this.$el.find('.cont-chart')[0]){ this.draw_zk_charts(this.$el.find('.cont-chart')[0], this.model.get('controller_total'), this.model.get('controller_offline_node'));}
    if (this.$el.find('.shard-chart')[0]){ this.draw_zk_charts(this.$el.find('.shard-chart')[0], this.model.get('shard_total'), this.model.get('shard_offline_node'));}
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
  },
  draw_zk_charts: function(target, total, offline){
    var options = {
      series: {
        pie: {
          show: true,
          radius: 1,
          innerRadius: 0.63,
          label: {
            show: false
          }
        }
      },
      legend: {
        show: false
      }
    };
    if (total == 0) {
      var data = [
        { label: "None", data: 1, color: "#CED7DA" }
      ];
    }
    else {
      var data = [
        { label: "Online", data: total - offline, color: "#5DB95D" },
        { label: "Offline", data: offline, color: "#CB4B4B" }
      ];
    }
    target.style.width = '135px';
    target.style.height = '135px';
    $.plot(target, data, options);
  }
});
