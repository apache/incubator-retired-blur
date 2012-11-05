var ControllerModel = Backbone.Model.extend({
  initialize: function(){
    this.view = new ControllerView({model: this});
    this.on('change', function(){
      this.view.render();
    });
  },
  remove: function(){
    if(this.get('status') == 1){
      this.destroy({
        success: function(){
          Notification("Successfully forgot the Controller!", true);
        }, 
        error: function(){
          Notification("Failed to forget the Controller", false);
        }
      });
    }
  }
});

var ControllerCollection = Backbone.StreamCollection.extend({
  url: "/zookeepers/" + CurrentZookeeper + "/controller/",
  model: ControllerModel,
  initialize: function(models, options){
    this.on('add', function(controller){
      if (this.length == 1){
        var table = $('#controllers table');
        $('.controller_table').delay(300).slideUp(500, function(){
          $('#controllers .no_children').hide();
          $('#controllers tbody').append(controller.view.render().$el);
          $(this).slideDown(500);
        });
      } else {
        $('#controllers tbody').append(controller.view.render().$el);
      }
    });
    this.on('remove', function(controller){
      if (this.length == 1){
        var table = $('#controllers table');
        $('.controller_table').delay(300).slideUp(500, function(){
          $('#controllers .no_children').show();
          controller.view.destroy();
          $(this).slideDown(500);
        });
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
    if (this.model.get('status') == 0){
      this.$el.attr('class', 'error error-failure');
    } else {
      this.$el.removeClass('error error-failure');
    }
    return this;
  },
  destroy_controller: function(){
    Confirm_Delete({
      message: "forget this controller",
      confirmed_action: _.bind(this.model.remove, this.model)
    });
  }
});