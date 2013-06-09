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
var ControllerModel = Backbone.Model.extend({
  initialize: function(){
    this.view = new ControllerView({model: this});
    this.on('change', function(){
      this.view.render();
    });
  },
  url: function(){
    return '/blur_controllers/' + this.get('id') + '.json';
  },
  remove: function(){
    if(this.get('controller_status') == 0){
      this.destroy({
        success: function(){
          Notification("Successfully forgot the Controller!", true);
        },
        error: function(){
          Notification("Failed to forget the Controller", false);
        }
      });
    } else {
      Notification("Cannot forget a Controller that is online!", false);
    }
  }
});

var ControllerCollection = Backbone.StreamCollection.extend({
  url: "/zookeepers/" + CurrentZookeeper + "/controller/",
  model: ControllerModel,
  initialize: function(models, options){
    this.on('add', function(controller){
      if (this.length == 1){
        $('#controllers .no_children').hide();
        $('#controllers tbody').append(controller.view.render().$el);
      } else {
        $('#controllers tbody').append(controller.view.render().$el);
      }
    });
    this.on('remove', function(controller){
      if (this.length == 0){
        $('#controllers .no_children').show();
        controller.view.destroy();
      } else {
        controller.view.destroy();
      }
    });
  }
});

var ControllerView = Backbone.View.extend({
  tagName: 'tr',
  template: JST['templates/environment/controller'],
  events:{
    "click .destroy-controller" : "destroy_controller"
  },
  render: function(){
    this.$el.attr('data-controller-id', this.model.get('id')).html(this.template({controller: this.model}));
    this.setRowStatus();
    return this;
  },
  setRowStatus: function(){
    switch(this.model.get('controller_status'))
    {
      case 0:
        this.$el.attr('class', 'error');
        return;
      case 1:
        this.$el.attr('class', '');
        return;
      case 2:
        this.$el.attr('class', 'warning');
        return;
    }
  },
  destroy_controller: function(){
    Confirm_Delete({
      message: "forget this controller",
      confirmed_action: _.bind(this.model.remove, this.model)
    });
  }
});