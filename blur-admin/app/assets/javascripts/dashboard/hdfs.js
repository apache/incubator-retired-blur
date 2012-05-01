var Hdfs = Backbone.Model.extend({
  initialize: function(){

  }
});

var HdfsCollection = Backbone.Collection.extend({
  model: Hdfs,
  url: Routes.hdfs_index_path({format: 'json'})
});

var HdfsView = Backbone.View.extend({
  className: 'zookeeper-info',
  events: {},
  template: JST['templates/dashboard/zookeeper'],
  render: function(){
    
  }
});