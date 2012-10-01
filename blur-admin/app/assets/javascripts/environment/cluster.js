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
    this.view = new ClusterCollectionView({collection: this}).render();
    this.on('add', function(cluster){
      var container = this.view.$el.find('tbody');
      container.find('.no_children').remove();
      container.append(cluster.view.render().$el);
    });
    this.on('remove', function(cluster){
      cluster.view.remove();
      if (this.length === 0){
        this.view.no_children();
      }
    });
  }
});

var ClusterCollectionView = Backbone.View.extend({
  tagName: 'table',
  className: 'table table-bordered',
  template: JST['templates/environment/cluster_collection'],
  render: function(){
    this.$el.html(this.template());
    if (this.collection.length == 0){
      this.no_children();
    } else {
      this.collection.each(_.bind(function(cluster){
        this.$el.find('tbody').append(cluster.view.render().$el);
      }, this));
    }
    return this;
  },
  no_children: function(){
    this.$el.find('tbody').html('<tr class="no_children"><td colspan="5">No Clusters!</td></tr>');
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