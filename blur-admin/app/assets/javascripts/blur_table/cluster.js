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
  },
  enable_tables: function(){
    var selected_tables = this.get('tables').where({state: 'disabled', checked: true});
    var table_ids = _.map(selected_tables, function(table){ return table.get('id'); });
    this.send_action_request(selected_tables, _.bind(function(){
      $().popup({
        title: "Enable Tables",
        titleClass: 'title',
        body: "Are you sure you want to enable these tables?",
        btns: {
          "Enable": {
            "class": "primary",
            func: _.bind(function() {
              $.ajax({
                type: 'PUT',
                url: Routes.enable_zookeeper_blur_tables_path(CurrentZookeeper),
                data: {tables: table_ids}
              });
              _.each(selected_tables, function(table){
                table.set({status: 5});
              });
              this.view.set_table_state();
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
    }, this));
  },
  disable_tables: function(){
    var selected_tables = this.get('tables').where({state: 'active', checked: true});
    var table_ids = _.map(selected_tables, function(table){ return table.get('id'); });
    this.send_action_request(selected_tables, _.bind(function(){
      $().popup({
        title: "Disable Tables",
        titleClass: 'title',
        body: "Are you sure you want to disable these tables?",
        btns: {
          "Disable": {
            "class": "primary",
            func: _.bind(function() {
              $.ajax({
                type: 'PUT',
                url: Routes.disable_zookeeper_blur_tables_path(CurrentZookeeper),
                data: {tables: table_ids}
              });
              _.each(selected_tables, function(table){
                table.set({status: 3});
              });
              this.view.set_table_state();
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
    }, this));
  },
  forget_tables: function(){
    var selected_tables = this.get('tables').where({state: 'deleted', checked: true});
    var table_ids = _.map(selected_tables, function(table){ return table.get('id'); });
    this.send_action_request(selected_tables, _.bind(function(){
      $().popup({
        title: "Forget Tables",
        titleClass: 'title',
        body: "Are you sure you want to forget these tables?",
        btns: {
          "Forget": {
            "class": "primary",
            func: _.bind(function() {
              $.ajax({
                type: 'DELETE',
                url: Routes.forget_zookeeper_blur_tables_path(CurrentZookeeper),
                data: {tables: table_ids}
              });
              _.each(selected_tables, function(table){
                table.view.remove();
                table.destroy();
              });
              this.view.set_table_state();
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
    }, this));
  },
  delete_tables: function(){
    var selected_tables = this.get('tables').where({state: 'disabled', checked: true});
    var table_ids = _.map(selected_tables, function(table){ return table.get('id'); });
    this.send_action_request(selected_tables, _.bind(function(){
      var delete_tables = function(delete_index) {
        $.ajax({
          type: 'DELETE',
          url: Routes.delete_zookeeper_blur_tables_path(CurrentZookeeper),
          data: {
            tables: table_ids,
            delete_index: delete_index
          }
        });
        _.each(selected_tables, function(table){
          table.set({status: 3});
        });
        this.view.set_table_state();
        $().closePopup();
      };
      $().popup({
        title: "Delete Tables",
        titleClass: 'title',
        body: "Do you want to delete all of the underlying table indicies?",
        btns: {
          "Delete tables and indicies": {
            "class": "danger",
            func: function() {
              _bind(delete_tables, this, true);
            }
          },
          "Delete tables only": {
            "class": "warning",
            func: function() {
              _bind(delete_tables, this, false);
            }
          },
          "Cancel": {
            func: function() {
              $().closePopup();
            }
          }
        }
      });
    }, this));
  },
  send_action_request: function(selected_tables, confirm_function){
    if (_.find(selected_tables, function(table){ return table.get('queried_recently'); })){
      $().popup({
        title: 'Warning! You are attempting to change an active table!',
        titleClass: 'title',
        body: 'You are attempting to perform an action on a recently queried table, Do you wish to continue?',
        btns: {
          "Continue": { class: 'danger', func: confirm_function },
          "Cancel": { func: function() { $().closePopup(); } }
        }
      });
    } else {
      confirm_function();
    }
  },
  perform_action: function(state, action){
    var selected_tables = this.get('tables').where({state: state, checked: true});
    if (_.find(selected_tables, function(table){ return table.get('queried_recently'); })){
      $().popup({
        title: 'Warning! You are attempting to ' + action + ' an active table!',
        titleClass: 'title',
        body: '<div>You are attempting to perform an action on a recently queried table, Do you wish to continue?</div>',
        btns: {
          "Enable": {
            class: 'danger',
            func: _.bind(function(){
              this.move_forward_with_action(action, selected_tables);
            }, this)
          },
          "Cancel": {
            func: function() {
              $().closePopup();
            }
          }
        }
      });
    } else {
      this.move_forward_with_action(action, selected_tables);
    }
  }
});

var ClusterCollection = Backbone.StreamCollection.extend({
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
    'click .check-all' : 'check_all_boxes',
    'click .btn[data-action=enable]' : 'enable_tables',
    'click .btn[data-action=disable]' : 'disable_tables',
    'click .btn[data-action=forget]' : 'forget_tables',
    'click .btn[data-action=delete]' : 'delete_tables'
  },
  colspan_lookup : {'active': 5, 'disabled': 3, 'deleted': 1},
  render: function(){
    this.$el.html(this.template({cluster: this.model}));
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
    this.set_table_values();
  },
  set_table_values: function(){
    var table_prefixes = ['active', 'disabled', 'deleted'];
    for (var index = 0; index < table_prefixes.length; index++){
      var table = this.$el.find('.' + table_prefixes[index] + '-table');
      table.find('.no-data').remove();
      var table_children_count = table.children().length;
      this.$el.find('.' + table_prefixes[index] + '-counter').text(table_children_count);
      if (this.model.get('tables').where({table: table_prefixes[index]}).length <= 0){
        table.append(this.no_table(this.colspan_lookup[table_prefixes[index]]));
      }
    }
  },
  set_table_state: function(){
    var checked_count = this.$el.find('tbody tr.highlighted-row').length;
    var set_checkbox_state = _.bind(function(){
      var row_count = this.$el.find('tbody:visible tr:not(.no-data, .changing-state)').length;
      var check_all = this.$el.find('.tab-pane.active .check-all');
      if (checked_count === row_count){
        if (checked_count === 0){
          check_all.removeAttr('checked');
          check_all.attr('disabled', 'disabled');
        } else {
          check_all.attr('checked', 'checked');
        }
      } else {
        check_all.removeAttr('checked');
      }
    }, this);
    var set_button_state = _.bind(function(){
      var toggle_button = this.$el.find('.tab-pane.active button');
      checked_count > 0 ? toggle_button.removeAttr('disabled') : toggle_button.attr('disabled', 'disabled');
    }, this);
    set_checkbox_state();
    set_button_state();
  },
  check_all_boxes: function(){
    var check_all = this.$el.find('.tab-pane.active .check-all');
    if (check_all.is(':checked')){
      this.$el.find('.tab-pane.active .bulk-action-checkbox:not(:checked)').click();
    } else {
      this.$el.find('.tab-pane.active .bulk-action-checkbox:checked').click();
    }
  },
  no_table: function(colspan){
    return $('<tr class="no-data"><td/><td colspan="' + colspan + '">No Tables for this Section</td></tr>')
  },
  enable_tables: function(event){
    this.model.enable_tables();
  },
  disable_tables: function(event){
    this.model.disable_tables();
  },
  forget_tables: function(event){
    this.model.forget_tables();
  },
  delete_tables: function(event){
    this.model.delete_tables();
  }
});