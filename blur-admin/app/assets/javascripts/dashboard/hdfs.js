//=require flot/flot
//=require flot/jquery.flot.pie.min.js

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
    if (this.$el.find('.hdfs-chart')[0]){ this.draw_hdfs_chart(this.$el.find('.hdfs-chart')[0]);}
    return this;
  },
  navigate_to_hdfs: function(){
    var id = this.model.get('id');
    window.location = Routes.hdfs_index_path() + '/' + id + '/show';
  },
  draw_hdfs_chart: function(target){
    var options = {
      series: {
        pie: {
          show: true,
          radius: 1,
          label: {
            show: false,
            radius: 3/4,
            formatter: function(label,series) {
              return '<div style="font-size: 8pt; text-align: center; padding: 2px; color: white;">'+label+'<br/></div>';
            },
            background: {
              opacity: 0.5,
              color: '#000'
            }
          }
        }
      },
      legend: {
        show: false
      }
    };
    var data = [
      { label: "Healthy", data: this.model.get('most_recent_stats').live_nodes, color: '#5DB95D' },
      { label: "Corrupt", data: this.model.get('corrupt_blocks'), color: '#AFD8F8' },
      { label: "Missing", data: this.model.get('missing_blocks'), color: '#CB4B4B' },
      { label: "Under-Rep", data: this.model.get('under_replicated'), color: '#EDC240' }
    ];
    console.log(this.model.get('most_recent_stats').live_nodes);
    target.style.width = '175px';
    target.style.height = '175px';
    $.plot(target, data, options);
  }
});
