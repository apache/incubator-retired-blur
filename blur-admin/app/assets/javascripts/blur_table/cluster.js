var Cluster = Backbone.Model.extend({
  defaults: {},
  initialize: function(){
    this.view = new ClusterView({model: this, id: 'cluster_' + this.id});
    this.build_child_tables();
    this.set_running_query_header_state();
    this.on('change:blur_tables', function(){
      this.update_child_tables();
    });
    this.on('change:safe_mode', function(){
      $('li#cluster_tab_' + this.get('id') + ' .safemode-icon').toggle();
    });
    this.on('table_has_been_queried', function(){
      this.set_running_query_header_state();
    });
  },
  build_child_tables: function(){
    var self = this;
    this.set({tables: new TableCollection(this.get('blur_tables'), {cluster: this})}, {silent: true});
  },
  update_child_tables: function(){
    this.get('tables').update(this.get('blur_tables'));
  },
  set_running_query_header_state: function(){
    var tables_queried = this.get('tables').where({queried_recently: true}).length;
    if (tables_queried > 0) {
      $('li#cluster_tab_' + this.get('id') + ' .queries-running-icon').show();
    } else {
      $('li#cluster_tab_' + this.get('id') + ' .queries-running-icon').hide();
    }
  }
});

var ClusterCollection = Backbone.Collection.extend({
  model: Cluster,
  url: Routes.zookeeper_blur_tables_path(CurrentZookeeper, {format: 'json'}),
  initialize: function(models, options){
    this.on('add', function(collection){
      var container = $('#tables-wrapper');
      var renderedView = $(collection.view.render().el);
      container.children('div').length === 0 ? container.html(renderedView.addClass('active')) : container.append(renderedView);
    });
  }
});

var ClusterView = Backbone.View.extend({
  className: 'tab-pane cluster',
  template: JST['templates/blur_table/cluster_view'],
  events: {
    'click .bulk-action-checkbox' : 'set_table_state',
    'click .check-all' : 'check_all_boxes'
  },
  render: function(){
    $(this.el).html(this.template({cluster: this.model}));
    this.populate_tables();
    return this;
  },
  populate_tables: function(){
    var el = $(this.el);
    this.model.get('tables').each(function(table){
      elementToAdd = table.view.el.rendered ? table.view.el : table.view.render().el;
      var table_parent = el.find('.' + table.get('table') + '-table');
      table_parent.append(elementToAdd);
      table_parent.siblings('thead').find('.check-all').removeAttr('disabled');
    });
    this.set_table_header_count();
  },
  set_table_header_count: function(){
    var el = $(this.el);
    var table_prefixes = ['active', 'disabled', 'deleted'];
    for (var index = 0; index < table_prefixes.length; index++){
      var table_children_count = el.find('.' + table_prefixes[index] + '-table').children().length;
      el.find('.' + table_prefixes[index] + '-counter').text(table_children_count);
    }
  },
  set_table_state: function(){
    var el = $(this.el);
    var checked_count = el.find('tbody tr.highlighted-row').length;
    var set_checkbox_state = function(){
      var row_count = el.find('tbody tr').length;
      var check_all = el.find('.tab-pane.active .check-all');
      checked_count === row_count ? check_all.attr('checked', 'checked') : check_all.removeAttr('checked');
    };
    var set_button_state = function(){
      var toggle_button = el.find('.tab-pane.active button');
      checked_count > 0 ? toggle_button.removeAttr('disabled') : toggle_button.attr('disabled', 'disabled');
    }
    set_checkbox_state();
    set_button_state();
  },
  check_all_boxes: function(){
    var el = $(this.el);
    var check_all = el.find('.tab-pane.active .check-all');
    if (check_all.is(':checked')){
      el.find('.tab-pane.active .bulk-action-checkbox:not(:checked)').click();
    } else {
      el.find('.tab-pane.active .bulk-action-checkbox:checked').click();
    }
  }
});