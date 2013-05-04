//= require flot/flot

var Table = Backbone.Model.extend({
  defaults: {
    'checked' : false,
  },
  state_lookup : ['deleted', 'deleting', 'disabled', 'disabling', 'active', 'enabling'],
  table_lookup : ['deleted', 'disabled', 'disabled', 'active', 'active', 'disabled'],
  colspan_lookup : {'active': 6, 'disabled': 5},
  initialize: function(){
    this.view = new TableView({model: this});
    this.view.render();
    this.set({
      state: this.state_lookup[this.get('table_status')],
      table: this.table_lookup[this.get('table_status')]
    });
    this.on('change:table_status', function(){
      var table = this.get('table')
      this.set({
        state: this.state_lookup[this.get('table_status')],
        table: this.table_lookup[this.get('table_status')],
        checked: false
      }, {
        silent: true
      });
      if (this.get('table') !== table){
        var table_parent = this.collection.cluster.view.$el.find('.' + this.get('table') + '-table');
        table_parent.append(this.view.el);
        table_parent.siblings('thead').find('.check-all').removeAttr('disabled');
        sorttable.makeSortable(table_parent.parent()[0]);
      }
    });
    this.on('change:queried_recently', function(){
      this.collection.cluster.trigger('table_has_been_queried');
    });
    this.on('change', function(){
      this.view.render();
    });
  },
  parse_uri: function(piece){
    var parse_url = /^(?:([A-Za-z]+):)?(\/{0,3})([0-9.\-A-Za-z]+)(?::(\d+))?(?:\/([^?#]*))?(?:\?([^#]*))?(?:#(.*))?$/;
    var result = parse_url.exec(this.get('table_uri'));
    var index = _.indexOf(['url', 'scheme', 'slash', 'host', 'port', 'path', 'query', 'hash'], piece);
    if (index === -1) throw 'The index, ' + piece + ' does not exist as a part of a uri.';
    return result[index];
  },
  get_terms: function(request_data, success){
    $.ajax({
      type: 'GET',
      url: Routes.terms_blur_table_path(this.get('id'), {format: 'json'}),
      data: request_data,
      success: success,
      error:function (xhr, ajaxOptions, thrownError){
        alert(thrownError + ": Terms Currently Unavailable");
      }
    });
  },
  capitalize_first: function(word){
    return word.charAt(0).toUpperCase() + word.slice(1);
  }
});

var TableCollection = Backbone.StreamCollection.extend({
  model: Table,
  initialize: function(models, options){
    this.cluster = options.cluster;
    this.on('add', function(table){
      var table_parent = table.collection.cluster.view.$el.find('.' + this.get('table') + '-table');
      table_parent.append(table.view.el);
      table_parent.siblings('thead').find('.check-all').removeAttr('disabled');
      sorttable.makeSortable(table_parent.parent()[0]);
    });
    this.on('remove', function(table){
      table.view.destroy();
    });
  }
});

var TableView = Backbone.View.extend({
  tagName: 'tr',
  className: 'blur_table',
  events: {
    'click .bulk-action-checkbox' : 'toggle_row',
    'click .hosts' : 'show_hosts',
    'click .info' : 'show_schema',
    'click .comments' : 'show_comments'
  },
  template: JST['templates/blur_table/table_row'],
  render: function(){
    this.rendered = true;
    this.$el.removeClass('changing-state')
    this.$el.attr('blur_table_id', this.model.get('id')).html(this.template({table: this.model})).removeClass('highlighted-row');
    if (this.model.get('checked')) this.$el.addClass('highlighted-row').find('.bulk-action-checkbox').prop('checked', 'checked');
    if (['disabling', 'enabling', 'deleting'].indexOf(this.model.get('state')) >= 0) this.$el.addClass('changing-state');
    return this;
  },
  toggle_row: function(){
    this.model.set({checked: !this.model.get('checked')}, {silent: true});
    this.$el.toggleClass('highlighted-row');
  },
  show_hosts: function(){
    $.ajax({
      type: 'GET',
      url: Routes.hosts_blur_table_path(this.model.get('id'), {format: 'json'}) ,
      success: _.bind(function(data){
        var host_modal = $(JST['templates/blur_table/hosts']({table: this.model, hosts: data}));
        this.setup_filter_tree(host_modal);
        $().popup({
          title: 'Additional Host/Shard Info',
          titleClass: 'title',
          body: host_modal
        });
      }, this)
    });
    return false;
  },
  show_schema: function(){
    $.ajax({
      type: 'GET',
      url: Routes.schema_blur_table_path(this.model.get('id'), {format: 'json'}) ,
      success: _.bind(function(data){
        var schema_modal = $(JST['templates/blur_table/schema']({table: this.model, schema: data}));
        this.setup_filter_tree(schema_modal.find('.table_info_tree'));
        $().popup({
          title: 'Additional Schema Info',
          titleClass: 'title',
          body: schema_modal,
        });
        var table_model = this.model;
        schema_modal.on('click', '.terms', function(){
          var clicked_element = $(this);
          var request_data =
          {
            family: $(this).attr('data-family-name'),
            column: $(this).attr('data-column-name'),
            startwith: ' ',
            size: 20
          };
          table_model.get_terms(request_data, _.bind(function(data) {
            new TermsView({
              clicked_element: clicked_element,
              parent: this,
              terms: data,
              family: request_data.family,
              column: request_data.column,
              table_id: this.get('id')})
            .render();
          }, table_model));
        });
      }, this)
    });
    return false;
  },
  show_comments: function(){
    var comment_modal = $(JST['templates/blur_table/comments']({table: this.model}));
    $().popup({
      title: 'Comments',
      titleClass: 'title',
      body: comment_modal ,
      btns: {
        "Submit": {
          "class": "primary",
          func: _.bind(function() {
            var input_comment = document.getElementById("comments").value;
            $.ajax({
              type: 'PUT',
              url: Routes.comment_blur_table_path(this.model.get('id'), {format: 'json'}) ,
              data: {comment: input_comment},
              success: _.bind(function(){
                this.model.set({comments: input_comment});
              }, this)
            });

            $().closePopup();
          }, this)
        },
        "Cancel": {
          func: function() {
            $().closePopup();
          }
        }
      }
    });
    return false;
  },
  setup_filter_tree: function(selector) {
    return selector.dynatree();
  }
});
