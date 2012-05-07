var Hdfs = Backbone.Model.extend({
  initialize: function(){
    this.view = new HdfsView({model: this});
    this.on('change', function(){
      this.view.render();
    });
  },
  node_width: function(){
    var stats = this.get('most_recent_stats');
    return Math.round((stats.live_nodes / stats.total_nodes) * 100);
  },
  usage_width: function(){
    var stats = this.get('most_recent_stats');
    return Math.round((stats.dfs_used / stats.config_capacity) * 100);
  },
  percent_used: function(){
    var usage_percent = this.usage_width();
    if (usage_percent < 1) {
      return '< 1%';
    }
    return usage_percent + '%';
  }
});

var HdfsCollection = Backbone.StreamCollection.extend({
  model: Hdfs,
  url: Routes.hdfs_index_path({format: 'json'}),
  initialize: function(models, options){
    this.on('add', function(hdfs){
      $('#hdfses').append(hdfs.view.render().el);
    });
    this.on('remove', function(hdfs){
      hdfs.view.remove();
      hdfs.destroy();
    });
  }
});

var HdfsView = Backbone.View.extend({
  className: 'hdfs_info online',
  events: {
    'click .hdfs-table' : 'navigate_to_hdfs'
  },
  template: JST['templates/dashboard/hdfs'],
  render: function(){
    this.$el.html(this.template({hdfs: this.model}));
    return this;
  },
  navigate_to_hdfs: function(){
    var id = this.model.get('id');
    window.location = Routes.hdfs_index_path() + '/' + id + '/show';
  }
});