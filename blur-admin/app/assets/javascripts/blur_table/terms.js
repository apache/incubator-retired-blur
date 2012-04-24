var TermsView = Backbone.View.extend({
  className: 'terms-view',
  template: JST['templates/blur_table/terms_view'],
  render: function(){
    this.popover = $('.terms').popover({
      title: this.options.column + " terms<i class='icon-remove popover-close' style='position:absolute; top:15px;right:15px'></i>",
      content: this.template(this.options),
      trigger: 'focus',
      placement: 'right'
    }).popover('show');

    // Declare all events separate of the events hash
    // because the popover clones the html and reference
    // is lost
    $('.popover').on('click', '.popover-close', _.bind(this.close_popover, this));
  },
  close_popover: function(){
    this.popover.popover('hide');
    this.remove();
  }
})