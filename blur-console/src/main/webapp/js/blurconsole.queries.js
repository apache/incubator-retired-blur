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
/*jshint laxbreak: true */
/*global blurconsole:false */
blurconsole.queries = (function() {
  'use strict';

  //------------------------ Configuration and State --------------------
  var configMap = {
    view: 'views/queries.tpl.html',
    states: ['Running', 'Interrupted', 'Complete', 'Back Pressure Interrupted'],
    queryDef: [
      { label: 'User', key: 'user', width: '0%', style: 'white-space:nowrap' },
      { label: 'Query', key: 'query', class: 'truncatedQuery', width: '100%' },
      { label: 'Started', width: '0%', style: 'white-space:nowrap', key: function(row) {
        var start = new Date(row.startTime);
        return start.toLocaleTimeString(); //start.getHours() + ':' + start.getMinutes() + ':' + start.getSeconds();
      } },
      { label: 'State', width: '0%', style: 'white-space:nowrap', key: function(row) {
        var stateInfo = configMap.states[row.state];
        if(row.state === 0 && row.percent) {
          stateInfo += ' <div class="badge badge-info">' + Math.round(row.percent) + '%</div>';
        }
        return stateInfo;
      } },
      { label: 'Actions', width: '0%', style: 'white-space:nowrap', key: function(row) {
        var actions = '';
        if(row.state === 0 && blurconsole.auth.hasRole('manager')) {
          actions += '<a href="#" class="cancelTrigger btn btn-danger" data-uuid="' + row.uuid + '" data-query="' + row.query + '" data-table="' + row.table + '"><i class="glyphicon glyphicon-ban-circle"></i> Cancel</a> ';
        }
        return actions;
      } }
    ],
  },
  stateMap = {
    $container: null,
    currentTable: null,
    currentFilter: null,
    currentSort: null
  },
  jqueryMap = {};

  //----------------------------------- Private methods ----------------------------

  function _setJqueryMap() {
    var $container = stateMap.$container;
    jqueryMap = {
      $container: $container,
      $tableHolder: $('#tableHolder'),
      $queryHolder: $('#queryHolder'),
      $filterHolder: $('#filterOptions'),
      $filterText: $('#filterOptions .filterText')
    };
  }

  function _registerPageEvents() {
    jqueryMap.$tableHolder.on('click', '.list-group-item', _showQueriesForTable);
    jqueryMap.$queryHolder.on('click', 'a.cancelTrigger', _cancelSelectedQuery);
    jqueryMap.$queryHolder.on('click', 'td.truncatedQuery', _stopTruncating);
    jqueryMap.$filterHolder.on('click', '.filterTrigger', _filterQueries);
  }

  function _unregisterPageEvents() {
    if(jqueryMap.$tableHolder) {
      jqueryMap.$tableHolder.off();
    }
  }

  function _waitForData() {
    var clusters = blurconsole.model.tables.getClusters();
    if(clusters && clusters.length > 0) {
      _drawTableList();
      _drawQueries();
    } else {
      setTimeout(_waitForData, 100);
    }
  }
  //----------------------------- Event Handlers and DOM Methods -----------------------------

  function _showQueriesForTable(evt) {
    evt.preventDefault();
    stateMap.currentTable = $(evt.currentTarget).attr('href');
    $('.list-group-item', jqueryMap.$tableHolder).removeClass('active');
    $('.list-group-item[href="' + stateMap.currentTable + '"]', jqueryMap.$tableHolder).addClass('active');
    _drawQueries();
    return false;
  }

  function _stopTruncating(evt) {
    $(evt.currentTarget).removeClass('truncatedQuery');
  }

  function _cancelSelectedQuery(evt) {
    var uuid = $(evt.currentTarget).data('uuid'),
      query = $(evt.currentTarget).data('query'),
      table = $(evt.currentTarget).data('table');
    var modalContent = blurconsole.browserUtils.modal('confirmDelete', 'Confirm Query Cancel', 'You are about to cancel the query [' + query + '].  Are you sure you want to do this?', [{
      classes: 'btn-primary killQuery',
      label: 'Stop Query'
    }, {
      classes: 'btn-default cancel',
      label: 'Cancel',
      data: {
        dismiss: 'modal'
      }
    }], 'medium');
    var modal = $(modalContent).modal().on('shown.bs.modal', function(e) {
      $(e.currentTarget).on('click', '.killQuery', function() {
        blurconsole.model.queries.cancelQuery(table, uuid);
        modal.modal('hide');
      });
    }).on('hidden.bs.modal', function(e) {
      $(e.currentTarget).remove();
    });
    return false;
  }

  function _filterQueries() {
    var filterVal = jqueryMap.$filterText.val();
    stateMap.currentFilter = filterVal;
    _drawQueries();
  }

  function _drawTableList() {
    var clusters = blurconsole.model.tables.getClusters();
    if(clusters) {
      jqueryMap.$tableHolder.html('');
      clusters.sort();
      $.each(clusters, function(i, cluster) {
        var panelContent, tables = blurconsole.model.tables.getEnabledTables(cluster);
        panelContent = '<div class="panel panel-default">' + '<div class="panel-heading">' + '<h3 class="panel-title">' + cluster + '</h3>' + '</div>' + '<div class="panel-body">';
        if(tables.length > 0) {
          tables.sort(function(a, b) {
            return a.name > b.name;
          });
          panelContent += '<div class="list-group">';
          $.each(tables, function(i, table) {
            panelContent += '<a href="' + table.name + '" class="list-group-item';
            if(table.name === stateMap.currentTable) {
              panelContent += ' active';
              _drawQueries();
            }
            panelContent += '">' + table.name + ' <i class="glyphicon glyphicon-exclamation-sign" data-table="' + table.name + '" style="display:none"></i></a>';
          });
          panelContent += '</div>';
        } else {
          panelContent += '<div class="alert alert-warning">There are not any enabled tables!</div>';
        }
        panelContent += '</div></div>';
        jqueryMap.$tableHolder.append(panelContent);
      });
    } else {
      jqueryMap.$tableHolder.html('<div class="alert alert-warning">There are no clusters of tables!</div>');
    }
  }

  function _drawQueries() {
    if(stateMap.currentTable) {
      jqueryMap.$queryHolder.html(blurconsole.browserUtils.table(configMap.queryDef, blurconsole.model.queries.queriesForTable(stateMap.currentTable, stateMap.currentSort, stateMap.currentFilter)));
    } else {
      jqueryMap.$queryHolder.html('<div class="alert alert-info">Select a table on the left to see the current queries</div>');
    }
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
    });
  }

  //-------------------------- Public API ---------------------------------

  function initModule($container) {
    $container.load(configMap.view, function() {
      stateMap.$container = $container;
      _setJqueryMap();
      $.gevent.subscribe(jqueryMap.$container, 'queries-updated', function() {
        _drawQueries();
        _updateActivityIndicators();
      });
      $.gevent.subscribe(jqueryMap.$container, 'tables-updated', _drawTableList);
      _registerPageEvents();
      _waitForData();
    });
    return true;
  }

  function unloadModule() {
    $.gevent.unsubscribe(jqueryMap.$container, 'queries-updated');
    $.gevent.unsubscribe(jqueryMap.$container, 'tables-updated');
    _unregisterPageEvents();
  }
  return {
    initModule: initModule,
    unloadModule: unloadModule
  };
}());