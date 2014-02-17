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
    return Math.round((stats.dfs_used_real / stats.config_capacity) * 100);
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
      hdfs.view.destroy();
    });
  }
});

var HdfsView = Backbone.View.extend({
  className: 'hdfs_info online',
  events: {
    'click' : 'navigate_to_hdfs'
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
