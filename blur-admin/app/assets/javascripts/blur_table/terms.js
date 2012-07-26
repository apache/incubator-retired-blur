var TermsView = Backbone.View.extend({
  className: 'terms-view',
  template: JST['templates/blur_table/terms_view'],
  render: function(){
    this.popover = this.options.clicked_element.popover({
      title: this.options.column + " terms<i class='icon-remove popover-close' style='position:absolute; top:15px;right:15px'></i>",
      content: this.template(this.options),
      trigger: 'focus',
      placement: 'right'
    }).popover('show');
    this.set_buttons_state();

    // Declare all events separate of the events hash
    // because the popover clones the html and reference
    // is lost
    $('.popover')
      .on('click', function(event){event.stopPropagation();})
      .on('click', '.popover-close', _.bind(this.close_popover, this))
      .on('click', '.more-terms-btn:not(.disabled)', _.bind(this.get_more_terms, this))
      .on('click', '.reset-terms', _.bind(this.refresh_list, this))
      .on('click', '.search-term-link', _.bind(this.redirect_to_search, this))
      .on('click', '.term-search-btn', _.bind(this.search_for_terms, this))
      .on('keydown', _.bind(this.search_using_enter, this));
    $('html').on('click', _.bind(this.close_popover, this));
  },
  close_popover: function(){
    this.popover.popover('hide');
    $('.popover').off('click');
    $('html').off('click');
    this.remove();
  },
  get_terms: function(request_data, success){
    this.options.parent.get_terms(request_data, success);
  },
  // Get another set of terms starting at the last list position
  // append it to the current list
  get_more_terms: function(){
    var spinner = Spinner.clone();
    $('.more-terms').append(spinner);
    var last_term = $('.term-li:last-child > span:last-child > span').text();
    this.get_terms({
      family: this.options.family,
      column: this.options.column,
      startwith: last_term,
      size: 21
    }, _.bind(function(data) {
      data.shift(1);
      spinner.remove();
      $('.popover .terms-list').append(JST['templates/blur_table/terms_list']({terms: data}));
    }, this));
  },
  // Get the first set of terms resetting the search
  // overwrites the list
  refresh_list: function(){
    var spinner = Spinner.clone();
    $('.more-terms').append(spinner);
    this.get_terms({
      family: this.options.family,
      column: this.options.column,
      startwith: ' ',
      size: 20
    }, _.bind(function(data) {
      spinner.remove();
      $('.popover .terms-list').html(JST['templates/blur_table/terms_list']({terms: data}));
    }, this));
  },
  search_for_terms: function(){
    var spinner = Spinner.clone();
    $('.more-terms').append(spinner);
    var last_term = $('.popover .term-search').val();
    this.options.parent.get_terms({
      family: this.options.family,
      column: this.options.column,
      startwith: last_term,
      size: 20
    }, _.bind(function(data) {
      spinner.remove();
      $('.popover .terms-list').html(JST['templates/blur_table/terms_list']({terms: data}));
      this.set_buttons_state();
    }, this));
  },
  search_using_enter: function(event){
    if (event.which === 13) {
      event.preventDefault();
      this.search_for_terms();
    }
  },
  redirect_to_search: function(event){
    var term = $(event.currentTarget).siblings('.input').children('span').text();
    window.location = Routes.zookeeper_searches_path(CurrentZookeeper)
      + ("?table_id=" + this.options.table_id + "&query=")
      + encodeURIComponent(this.options.family + "." + this.options.column + ":" + term);
  },
  set_buttons_state: function(){
    if ($('.popover .terms-list').children().length < 20){
      $('.popover .more-terms-btn').addClass('disabled');
    }
  }
})