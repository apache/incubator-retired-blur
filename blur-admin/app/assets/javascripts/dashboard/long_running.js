var LongRunningView = Backbone.View.extend({
  tagName: 'ul',
  className: 'modal-list',
  events: {
    'click .icon-remove' : 'cancel_query'
  },
  template: JST['templates/dashboard/long_running'],
  render: function(data){
    this.$el.html(this.template({data: data}));
    return this;
  },
  cancel_query: function(event){
    var self = $(event.target);
    var id = $(event.target.parentElement).attr('data-id');
    $.ajax({
      type: 'PUT',
      url: Routes.zookeeper_blur_query_path(CurrentZookeeper, id),
      data: {cancel: true},
      success: function(){
        self.closest('li').remove();
      }
    });
  }
});