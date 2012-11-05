var ClusterModel = Backbone.Model.extend({
  initialize: function(){
    this.view = new ClusterView({model: this});
    this.on('change', function(){
      this.view.render();
    });
  },
  safe_mode: function(){
    return this.get('safe_mode') ? 'Yes' : 'No';
  },
  remove: function(){
    if(this.get('status') == 0){
      this.destroy({
        success: function(){
          Notification("Successfully forgot the Cluster!", true);
        },
        error: function(){
          Notification("Failed to forget the Cluster", false);
        }
      });
    }
  }
});

var ClusterCollection = Backbone.StreamCollection.extend({
  url: "/zookeepers/" + CurrentZookeeper + "/cluster/",
  model: ClusterModel,
  initialize: function(models, options){
    this.on('add', function(clusters){
      if (this.length == 1){
        var table = $('#clusters table');
        $('.clusters_table').delay(300).slideUp(500, function(){
          $('#clusters .no_children').hide();
          $('#clusters tbody').append(clusters.view.render().$el);
          $(this).slideDown(500);
        });
      } else {
        $('#clusters tbody').append(clusters.view.render().$el);
      }
    });
    this.on('remove', function(clusters){
      if (this.length == 1){
        var table = $('#clusterss table');
        $('.clusters_table').delay(300).slideUp(500, function(){
          $('#clusters .no_children').show();
          clusters.view.destroy();
          $(this).slideDown(500);
        });
      } else {
        clusters.view.destroy();
      }
    });
  }
});

var ClusterView = Backbone.View.extend({
  tagName: 'tr',
  template: JST['templates/environment/cluster'],
  events:{
    "click .more-shard-info" : "show_shards",
    "click .destroy-cluster" : "destroy_cluster"
  },
  render: function(){
    this.$el.attr('data-cluster-id', this.model.get('id')).html(this.template({cluster: this.model}));
    return this;
  },
  show_shards: function(event){
    new ShardCollection(null, {cluster_id: this.model.get('id')});
  },
  destroy_cluster: function(){
    Confirm_Delete({
      message: "forget this cluster",
      confirmed_action: _.bind(this.model.remove, this.model)
    });
  }
});