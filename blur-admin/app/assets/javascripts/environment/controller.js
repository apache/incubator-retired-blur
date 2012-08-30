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
    this.view = new ControllerCollectionView({collection: this}).render();
    this.on('add', function(controller){
      var container = this.view.$el.find('tbody');
      container.find('.no_children').remove();
      container.append(controller.view.render().$el);
    });
    this.on('remove', function(controller){
      controller.view.remove();
      if (this.length == 0){
        this.view.no_children();
      }
    });
  }
});

var ControllerCollectionView = Backbone.View.extend({
  tagName: 'table',
  className: 'table table-bordered',
  template: JST['templates/environment/controller_collection'],
  render: function(){
    this.$el.html(this.template());
    if (this.collection.length == 0){
      this.no_children();
    } else {
      this.collection.each(_.bind(function(controller){
        this.$el.find('tbody').append(controller.view.render().$el);
      }, this));
    }
    return this;
  },
  no_children: function(){
    this.$el.find('tbody').html('<tr class="no_children"><td colspan="3">No Controllers!</td></tr>');
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