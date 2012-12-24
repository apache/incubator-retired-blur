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
    if(this.get('status') == 0){
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
      if (this.length == 1){
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
    switch(this.model.get('status'))
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