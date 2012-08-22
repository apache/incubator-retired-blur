var ShardModel = Backbone.Model.extend({
  initialize: function(){
    this.view = new ShardView({model: this});
  },
  status: function(){
    var statusString = "Shard: " + this.get('node_name');
    statusString += " | Blur Version: " + this.get('blur_version');
    statusString += " | Status: " + (this.get('status') ? 'Online' : 'Offline');
    return statusString;
  }
});

var ShardCollection = Backbone.Collection.extend({
  model: ShardModel,
  initialize: function(models, options){
    this.url = Routes.shards_zookeeper_path(CurrentZookeeper, options.cluster_id);
    this.view = new ShardCollectionView();
    this.on('add', function(shard){
      this.view.$el.append(shard.view.render().$el);
    });
    this.fetch({
      success: _.bind(this.sort_and_draw, this)
    });
  },
  sort_and_draw: function(){
    var sorted_models = this.sortBy(function(shard){
      return shard.get('node_name');
    });
    _.each(sorted_models, _.bind(function(shard){
      this.view.$el.append(shard.view.render().$el);
    }, this));
    this.view.show_shards();
  }
});

var ShardView = Backbone.View.extend({
  tagName: 'li',
  template: JST['templates/environment/shard'],
  render: function(){
    errorClass = this.model.get('status') ? 'no-error' : 'error';
    this.$el.attr('class', errorClass);
    this.$el.html(this.template({shard: this.model}));
    return this;
  }
});

var ShardCollectionView = Backbone.View.extend({
  tagName: 'ul',
  className: 'modal-list no-well',
  show_shards: function(){
    $().popup({
      title: "Shards",
      titleClass: 'title',
      body: this.$el
    });
    this.$el.find('.icon').tooltip();
  }
});