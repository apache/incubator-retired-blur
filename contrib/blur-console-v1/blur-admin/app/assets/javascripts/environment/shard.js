/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var ShardModel = Backbone.Model.extend({
  initialize: function(){
    this.view = new ShardView({model: this});
  },
  url: function(){
    return '/blur_shards/' + this.get('id') + '.json';
  },
  status: function(){
    var statusString = this.get('node_name');
    statusString += " | Blur Version: " + this.get('blur_version');
    statusString += " | Status: " + this.onlineStatus();
    if (this.get('shard_status') != 1) {
      statusString += " at " + this.offlineDate();
    }
    return statusString;
  },
  onlineStatus: function(){
    switch(this.get('shard_status'))
    {
      case 0:
        return "Offline"
      case 1:
        return "Online"
      case 2:
        return "Quorum Issue"
    }
  },
  remove: function(){
    if(this.get('shard_status') == 0){
      this.destroy({
        success: function(){
          Notification("Successfully forgot the Shard!", true);
        },
        error: function(){
          Notification("Failed to forget the Shard", false);
        }
      });
    } else {
      Notification("Cannot forget a Shard that is online!", false);
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
    return [shard.get('shard_status'), shard.get('node_name')];
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
    errorClass = (this.model.get('shard_status') == 1) ? 'no-error' : 'error';
    this.$el.attr('class', errorClass);
    this.$el.html(this.template({shard: this.model}));
    return this;
  },
  destroy_shard: function(){
    Confirm_Delete({
      message: "forget this shard",
      confirmed_action: _.bind(this.model.remove, this.model)
    });
  }
});