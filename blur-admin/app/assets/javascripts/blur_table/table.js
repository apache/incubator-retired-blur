var Table = Backbone.Model.extend({
  defaults: {
    'checked' : false,
  },
  state_lookup : ['deleted', 'deleting', 'disabled', 'disabling', 'active', 'enabling'],
  table_lookup : ['deleted', 'disabled', 'disabled', 'active', 'active', 'disabled'],
  colspan_lookup : {'active': 5, 'disabled': 3, 'deleted': 1},
  initialize: function(){
    this.view = new TableView({model: this});
    this.set({state: this.state_lookup[this.get('status')], table: this.table_lookup[this.get('status')]});
    this.on('change:status', function(){
      this.set({state: this.state_lookup[this.get('status')], table: this.table_lookup[this.get('status')], checked: false}, {silent: true});
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
    var server = JSON.parse(this.get('server'));
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
  capitalize_first: function(word){
    return word.charAt(0).toUpperCase() + word.slice(1);
  }
});

var TableCollection = Backbone.Collection.extend({
  model: Table,
  initialize: function(models, options){
    this.cluster = options.cluster;
  }
});

var TableView = Backbone.View.extend({
  tagName: 'tr',
  className: 'blur_table',
  events: {
    'click .bulk-action-checkbox' : 'toggle_row'
  },
  template: JST['templates/blur_table/table_row'],
  render: function(){
    var el = $(this.el);
    this.rendered = true;
    el.attr('blur_table_id', this.model.get('id')).html(this.template({table: this.model})).removeClass('highlighted-row');
    if (this.model.get('checked')) el.addClass('highlighted-row').find('.bulk-action-checkbox').prop('checked', 'checked');
    return this;
  },
  toggle_row: function(){
    this.model.set({checked: !this.model.get('checked')}, {silent: true});
    $(this.el).toggleClass('highlighted-row');
  }
});