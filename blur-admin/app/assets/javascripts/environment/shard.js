var ShardModel = Backbone.Model.extend({
  initialize: function(){
    this.view = new ShardView({model: this});
  },
  status: function(){
    var statusString = "Shard: " + this.get('node_name');
    statusString += " | Blur Version: " + this.get('blur_version');
    statusString += " | Status: " + this.onlineStatus();
    if (this.get('status') != 1) {
      statusString += " at " + this.offlineDate();
    }
    return statusString;
  },
  onlineStatus: function(){
    switch(this.get('status'))
    {
      case 0:
        return "Offline"
      case 1:
        return "Online"
      case 2:
        return "Quorum Issue"
    }
  },
  offlineDate: function(){
    var date = new Date(this.get('updated_at').toLocaleString())
    var formattedDate = date.getMonth() + '/' + date.getDay() + '/' + date.getFullYear();
    var formattedTime = date.getHours() + ':' + date.getMinutes();
    return formattedDate + ' ' + formattedTime;
  }
});

var ShardCollection = Backbone.Collection.extend({
  model: ShardModel,
  initialize: function(models, options){
    this.url = Routes.cluster_blur_shards_path(options.cluster_id, {format: 'json'});
    this.view = new ShardCollectionView({collection: this});
    this.fetch({
      success: _.bind(function(){
        this.view.render();
      }, this)
    });
  },
  comparator: function(shard){
    return shard.get('node_name');
  }
});

var ShardCollectionView = Backbone.View.extend({
  tagName: 'ul',
  className: 'modal-list no-well',
  render: function(){
    this.collection.each(_.bind(function(shard){
      this.$el.append(shard.view.render().$el);
    }, this));
    this.show_shards();
  },
  show_shards: function(){
    $().popup({
      title: "Shards",
      titleClass: 'title',
      body: this.$el
    });
    this.$el.find('.icon').tooltip();
  }
});

var ShardView = Backbone.View.extend({
  tagName: 'li',
  template: JST['templates/environment/shard'],
  events:{
    "click .icon" : "destroy_shard"
  },
  render: function(){
    errorClass = (this.model.get('status') == 1) ? 'no-error' : 'error';
    this.$el.attr('class', errorClass);
    this.$el.html(this.template({shard: this.model}));
    return this;
  }, 
  destroy_shard: function(){
    Confirm_Delete({
      message: "forget this shard",
      confirmed_action: _.bind(this.model.destroy, this.model)
    });
  }
});