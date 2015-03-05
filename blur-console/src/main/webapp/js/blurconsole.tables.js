/*

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

/*global blurconsole:false, alert:false */
blurconsole.tables = (function () {
  'use strict';

  function _tableName(row) {
    var activityIndicator = '';
    var errorIndicator = '';
    if(row.enabled) {
      activityIndicator = ' <i class="glyphicon glyphicon-exclamation-sign" data-table="' + row.name + '" style="display:none" title="Activity detected"></i>';
    }
    if(row.error) {
      errorIndicator = ' <i class="glyphicon glyphicon-ban-circle" title="'+row.error+'"></i>';
    }
    return row.name + activityIndicator + errorIndicator;
  }
    //------------------------ Configuration and State ----------------------
  var configMap = {
    view : 'views/tables.tpl.html',
    enabledDef : [
      {label:'Table Name', key: _tableName},
      {label:'Row Count', key: 'rows', format:'number'},
      {label:'Record Count', key: 'records', format:'number'},
      {label:'Actions', key: function(row) {
        var actions = '', table = row.name;
        actions += '<a href="#" class="schemaTrigger btn btn-default" data-name="' + table + '"><i class="glyphicon glyphicon-list-alt"></i> Schema</a> ';
        if(blurconsole.auth.hasRole('manager')) {
          actions += '<a href="#" class="copyTrigger btn btn-default" data-name="' + table + '"><i class="glyphicon glyphicon-export"></i> Copy</a> ';
          actions += '<a href="#" class="disableTrigger btn btn-danger" data-name="' + table + '"><i class="glyphicon glyphicon-cloud-download"></i> Disable</a> ';
        }
        return actions;
      }}
    ],
    disabledDef : [
      {label:'Table Name', key:_tableName},
      {label:'Actions', key: function(row) {
        var actions = '', table = row.name;
        if(blurconsole.auth.hasRole('manager')) {
          actions += '<a href="#" class="enableTrigger btn btn-default" data-name="' + table + '"><i class="glyphicon glyphicon-cloud-upload"></i> Enable</a> ';
          actions += '<a href="#" class="deleteTrigger btn btn-danger" data-name="' + table + '"><i class="glyphicon glyphicon-trash"></i> Delete</a> ';
        }
        return actions;
      }}
    ]
  },
  stateMap = { $container : null },
  jqueryMap = {};

  //----------------------------- Private Methods ---------------------------
  function _setJqueryMap() {
    var $container = stateMap.$container;
    jqueryMap = {
      $container : $container,
      $tableInfoHolder : $('#tableInfoHolder'),
      $tables : {}
    };
  }

  function _waitForData() {
    if (blurconsole.model.tables.getClusters().length > 0) {
      _buildTabs();
    } else {
      setTimeout(_waitForData, 100);
    }
  }

  function _registerPageEvents() {
    // Tab control
    jqueryMap.$tableInfoHolder.on('click', 'ul.nav a', function(e) {
      e.preventDefault();
      $(this).tab('show');
    });

    // View Schema
    jqueryMap.$tableInfoHolder.on('click', 'a.schemaTrigger', function() {
      $.gevent.publish('schema-show', $(this).data('name'));
      return false;
    });

    // Disable Table
    jqueryMap.$tableInfoHolder.on('click', 'a.disableTrigger', function() {
      blurconsole.model.tables.disableTable($(this).data('name'));
      return false;
    });

    // Enable Table
    jqueryMap.$tableInfoHolder.on('click', 'a.enableTrigger', function() {
      blurconsole.model.tables.enableTable($(this).data('name'));
      return false;
    });

    // Delete Table
    jqueryMap.$tableInfoHolder.on('click', 'a.deleteTrigger', function() {
      var tableName = $(this).data('name');
      var modalContent = blurconsole.browserUtils.modal('confirmDelete', 'Confirm Table Deletion', 'You are about to delete table ' + tableName + '.  Are you sure you want to do this? If so, do you also want to delete the underlying table data?', [
        {classes: 'btn-warning tableOnly', label: 'Table Only'},
        {classes: 'btn-danger tableAndData', label: 'Table And Data'},
        {classes: 'btn-default cancel', label: 'Cancel', data: {dismiss:'modal'}}
      ], 'medium');

      var modal = $(modalContent).modal().on('shown.bs.modal', function(e){
        $(e.currentTarget).on('click', '.tableOnly', function() {
          blurconsole.model.tables.deleteTable(tableName, false);
          modal.modal('hide');
        }).on('click', '.tableAndData', function() {
          blurconsole.model.tables.deleteTable(tableName, true);
          modal.modal('hide');
        });
      }).on('hidden.bs.modal', function(e) {
        $(e.currentTarget).remove();
      });
      return false;
    });

    // Copy Table
    jqueryMap.$tableInfoHolder.on('click', 'a.copyTrigger', function() {
      var tableName = $(this).data('name');

      var modalBody = '<form><div class="form-group"><label for="clusters">Destination Cluster:</label><select id="clusters" class="form-control">';
      $.each(blurconsole.model.tables.getClusters(), function(i, cluster) {
        modalBody += '<option value="' + cluster + '">' + cluster + '</option>';
      });
      modalBody += '</select></div><div class="form-group"><label for="newName">New Table Name:</label><input type="text" class="form-control" id="newName"/></div>';
      modalBody += '<div class="form-group"><label for="newName">New Table Location:</label><input type="text" class="form-control" id="newLocation"/></div></form>';

      var modalContent = blurconsole.browserUtils.modal('copyInfo', 'Where do you want to copy table ' + tableName, modalBody, [
        {classes: 'btn-primary copyTable', label: 'Copy'},
        {classes: 'btn-default cancel', label: 'Cancel', data: {dismiss:'modal'}}
      ], 'medium');

      var modal = $(modalContent).modal().on('shown.bs.modal', function(e) {
        $(e.currentTarget).on('click', '.copyTable', function() {
          var newTableName = $('#newName').val();
          var newCluster = $('#clusters').val();
          var newLocation = $('#newLocation').val();

          if (newTableName === '' || newLocation === '') {
            alert('New Table Name and New Table Location are required');
          } else if (blurconsole.model.tables.getTableNamesForCluster(newCluster).indexOf(newTableName) >= 0) {
            alert('Table ' + newTableName + ' already exists in cluster ' + newCluster);
          } else {
            blurconsole.model.tables.copyTable(tableName, newTableName, newLocation, newCluster);
            modal.modal('hide');
          }
        });
      }).on('hidden.bs.modal', function(e) {
        $(e.currentTarget).remove();
      });
      return false;
    });
  }

  function _unregisterPageEvents() {
    if (jqueryMap.$tableInfoHolder) {
      jqueryMap.$tableInfoHolder.off();
    }
  }

    //------------------------- Event Handling and DOM Methods ---------------------------
  function _buildTabs() {
    var clusters = blurconsole.model.tables.getClusters();
    var needsTabs = clusters.length > 1;

    if (needsTabs) {
      var tabMarkup = '<ul class="nav nav-tabs">';
      tabMarkup += $.map(clusters, function(cluster, idx) {
        return '<li class="' + (idx === 0 ? 'active' : '') + '"><a href="#' + cluster + '_pane" data-toggle="tab">' + cluster + ' <i class="glyphicon glyphicon-exclamation-sign" style="display:none" title="Activity detected"></i></a></li>';
      }).join('');
      tabMarkup += '</ul>';

      jqueryMap.$tableInfoHolder.html($(tabMarkup));
    }

    var paneMarkup = needsTabs ? '<div class="tab-content">' : '';
    paneMarkup += $.map(clusters, function(cluster, idx) {
      return '<div id="' + cluster + '_pane" class="tab-pane' + (idx === 0 ? ' active' : '') + '"><h3>Enabled Tables</h3><div class="enabledSection"></div><h3>Disabled Tables</h3><div class="disabledSection"></div></div>';
    }).join('');
    paneMarkup += needsTabs ? '</div>' : '';

    if (needsTabs) {
      jqueryMap.$tableInfoHolder.append(paneMarkup);
    } else {
      jqueryMap.$tableInfoHolder.html(paneMarkup);
    }

    $.each(clusters, function(idx, cluster){
      var clusterPane = $('#' + cluster + '_pane');
      clusterPane.find('.enabledSection').html(blurconsole.browserUtils.table(configMap.enabledDef, blurconsole.model.tables.getEnabledTables(cluster)));
      clusterPane.find('.disabledSection').html(blurconsole.browserUtils.table(configMap.disabledDef, blurconsole.model.tables.getDisabledTables(cluster)));
    });
  }

  function _updateTableList() {
    var clusters = blurconsole.model.tables.getClusters();

    $.each(clusters, function(idx, cluster) {
      var clusterPane = $('#' + cluster + '_pane'), enabledSection, disabledSection;
      enabledSection = clusterPane.find('.enabledSection');
      disabledSection = clusterPane.find('.disabledSection');

      if (enabledSection.length > 0) {
        enabledSection.html(blurconsole.browserUtils.table(configMap.enabledDef, blurconsole.model.tables.getEnabledTables(cluster)));
      }
      if (disabledSection.length > 0) {
        disabledSection.html(blurconsole.browserUtils.table(configMap.disabledDef, blurconsole.model.tables.getDisabledTables(cluster)));
      }
    });
  }

  function _updateActivityIndicators() {
    var clusters = blurconsole.model.tables.getClusters();

    $.each(clusters, function(i, cluster) {
      var clusterHasActivity = false,
        tables = blurconsole.model.tables.getEnabledTables(cluster);

      $.each(tables, function(i, table){
        if (blurconsole.model.queries.tableHasActivity(table.name)) {
          clusterHasActivity = true;
          $('i[data-table="' + table.name + '"]').show();
        } else {
          $('i[data-table="' + table.name + '"]').hide();
        }
      });

      if (clusterHasActivity) {
        $('a[href="#' + cluster +'_pane"] i').show();
      } else {
        $('a[href="#' + cluster +'_pane"] i').hide();
      }
    });
  }

    //-------------------------- Public API ------------------------------
  function unloadModule() {
    $.gevent.unsubscribe(jqueryMap.$container, 'tables-updated');
    $.gevent.unsubscribe(jqueryMap.$container, 'queries-updated');
    _unregisterPageEvents();
  }

  function initModule($container) {
    $container.load(configMap.view, function() {
      stateMap.$container = $container;
      _setJqueryMap();
      $.gevent.subscribe(jqueryMap.$container, 'tables-updated', _updateTableList);
      $.gevent.subscribe(jqueryMap.$container, 'queries-updated', _updateActivityIndicators);
      _waitForData();
      _registerPageEvents();
    });
    return true;
  }

  return {
    initModule : initModule,
    unloadModule : unloadModule
  };
}());