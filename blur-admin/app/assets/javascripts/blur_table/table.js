//= require flot/flot

var Table = Backbone.Model.extend({
  defaults: {
    'checked' : false,
  },
  state_lookup : ['deleted', 'deleting', 'disabled', 'disabling', 'active', 'enabling'],
  table_lookup : ['deleted', 'disabled', 'disabled', 'active', 'active', 'disabled'],
  colspan_lookup : {'active': 6, 'disabled': 3, 'deleted': 1}, //changed active from 5 to 6 for spark
  initialize: function(){
    this.view = new TableView({model: this});
    this.set({
      state: this.state_lookup[this.get('status')],
      table: this.table_lookup[this.get('status')]
    });
    this.on('change:status', function(){
      this.set({
        state: this.state_lookup[this.get('status')],
        table: this.table_lookup[this.get('status')],
        checked: false
      }, {
        silent: true
      });
      this.collection.cluster.view.populate_tables();
    });
    this.on('change:queried_recently', function(){
      this.collection.cluster.trigger('table_has_been_queried');
    });
    this.on('change', function(){
      this.view.render();
    });
  },
  host_shard_string: function(){
    var server = this.get('hosts');
    if (server) {
      var count = 0;
      var hosts = 0;
      for (var key in server) {
        hosts++;
        count += server[key].length;
      }
      return hosts + ' | ' + count;
    } else {
      return "Schema Information is Currently Unavailable";
    }
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
      url: Routes.terms_zookeeper_blur_table_path(CurrentZookeeper, this.get('id'), {format: 'json'}),
      data: request_data,
      success: success
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
  }
});

var TableView = Backbone.View.extend({
  tagName: 'tr',
  className: 'blur_table',
  events: {
    'click .bulk-action-checkbox' : 'toggle_row',
    'click .hosts' : 'show_hosts',
    'click .info' : 'show_schema'
  },
  template: JST['templates/blur_table/table_row'],
  render: function(){
    this.rendered = true;
    this.$el.removeClass('changing-state')
    this.$el.attr('blur_table_id', this.model.get('id')).html(this.template({table: this.model})).removeClass('highlighted-row');
    if (this.model.get('checked')) this.$el.addClass('highlighted-row').find('.bulk-action-checkbox').prop('checked', 'checked');
    if (['disabling', 'enabling', 'deleting'].indexOf(this.model.get('state')) >= 0) this.$el.addClass('changing-state');
    this.draw_query_spark_line(this.model.get('sparkline'), this.$el.find('.spark_line')[0]);
    return this;
  },
  draw_query_spark_line: function(data, target){
    var options = {
      xaxis: {
        max: 10,
        min: -1
      },
      grid: {
        show: false
      }
    };
    var series = [{
      data: data,
      color: '#000000',
      shadowSize: 0
      },{
      data: [ data[ data.length -1 ] ],
      points: {
        show: true,
        radius: 1,
        fillColor: '#ff0000'
      },
      color: '#ff0000'
    }];
    target.style.width = '120px';
    target.style.height = '20px';
    $.plot(target, series, options);
  },
  toggle_row: function(){
    this.model.set({checked: !this.model.get('checked')}, {silent: true});
    this.$el.toggleClass('highlighted-row');
  },
  show_hosts: function(){
    var host_modal = $(JST['templates/blur_table/hosts']({table: this.model}));
    this.setup_filter_tree(host_modal);
    $().popup({
      title: 'Additional Host/Shard Info',
      titleClass: 'title',
      body: host_modal
    });
  },
  show_schema: function(){
    var schema_modal = $(JST['templates/blur_table/schema']({table: this.model}));
    this.setup_filter_tree(schema_modal.find('.table_info_tree'));
    $().popup({
      title: 'Additional Schema Info',
      titleClass: 'title',
      body: schema_modal
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
  },
  setup_filter_tree: function(selector) {
    return selector.dynatree();
  }
});
