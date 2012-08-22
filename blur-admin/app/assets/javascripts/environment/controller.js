var ControllerModel = Backbone.Model.extend({
  initialize: function(){
    this.view = new ControllerView({model: this});
    this.on('change', function(){
      this.view.render();
    });
  }
});

var ControllerCollection = Backbone.StreamCollection.extend({
  model: ControllerModel,
  initialize: function(models, options){
    this.view = new ControllerCollectionView().render();
    this.on('add', function(controller){
      var container = this.view.$el.find('tbody');
      container.append(controller.view.render().$el);
    });
  }
});

var ControllerCollectionView = Backbone.View.extend({
  tagName: 'table',
  className: 'table table-bordered',
  template: JST['templates/environment/controller_collection'],
  render: function(){
    this.$el.html(this.template());
    return this;
  }
});

var ControllerView = Backbone.View.extend({
  tagName: 'tr',
  template: JST['templates/environment/controller'],
  render: function(){
    this.$el.attr('data-controller-id', this.model.get('id')).html(this.template({controller: this.model}));
    return this;
  }
});