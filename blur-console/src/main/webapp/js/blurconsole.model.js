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
/*global blurconsole:false */
blurconsole.model = (function() {
  'use strict';

  //----------------------- Configuration and State ------------------
  var
    configMap = {
      poller : null
    },
    stateMap = {
      currentTables: null,
      currentClusters: [],
      nodeMap : null,
      queryPerformance : [],
      queries : {},
      errors: [],
      schema: {}
    };

  //----------------------- Models ----------------------------------
  var tables = (function() {
    //-------------- Public API -----------------
    function getClusters() {
      if (stateMap.currentClusters === null) {
        return [];
      }

      return blurconsole.utils.unique(stateMap.currentClusters, true);
    }

    function getEnabledTables(cluster) {
      var data = [];

      $.each(stateMap.currentTables, function(idx, table) {
        if (table.cluster === cluster && table.enabled) {
          data.push(table);
        }
      });

      return data;
    }

    function getDisabledTables(cluster) {
      var data = [];

      $.each(stateMap.currentTables, function(idx, table) {
        if (table.cluster === cluster && !table.enabled) {
          data.push(table);
        }
      });

      return data;
    }

    function getAllEnabledTables() {
      var tableMap = {};

      $.each(getClusters(), function(c, cluster){
        tableMap[cluster] = getEnabledTables(cluster);
      });

      return tableMap;
    }

    function getTableNamesForCluster(cluster) {
      var list = [];
      $.each(stateMap.currentTables, function(idx, table) {
        if (table.cluster === cluster) {
          list.push(table.name);
        }
      });

      return list;
    }

    function isDataLoaded() {
      return stateMap.currentTables !== null;
    }

    function disableTable(tableName) {
      configMap.poller.disableTable(tableName);
    }

    function enableTable(tableName) {
      configMap.poller.enableTable(tableName);
    }

    function deleteTable(tableName, includeFiles) {
      configMap.poller.deleteTable(tableName, includeFiles);
    }

    function getSchema(tableName, callback) {
      if(stateMap.schema && stateMap.schema.tableName === tableName && new Date() - stateMap.schema.date < 60000) {
        setTimeout(function(){
          callback(stateMap.schema.data);
        }, 0);
      } else {
        configMap.poller.getSchema(tableName, function(schema) {
          stateMap.schema.tableName = tableName;
          stateMap.schema.data = schema;
          stateMap.schema.date = new Date();
          callback(schema);
        });
      }
    }

    function getFamilies(tableName) {
      var table;

      $.each(stateMap.currentTables, function(idx, t) {
        if (t.name === tableName) {
          table = t;
          return false;
        }
      });
      if(table) {
        return table.families;
      } else {
        return [];
      }
    }

    function findTerms(table, family, column, startsWith, callback) {
      configMap.poller.findTerms(table, family, column, startsWith, callback);
    }

    function copyTable(srcTable, destTable, destLocation, cluster) {
      configMap.poller.copyTable(srcTable, destTable, destLocation, cluster);
    }

    return {
      getClusters : getClusters,
      getEnabledTables : getEnabledTables,
      getDisabledTables : getDisabledTables,
      isDataLoaded : isDataLoaded,
      disableTable : disableTable,
      enableTable : enableTable,
      deleteTable : deleteTable,
      getSchema : getSchema,
      findTerms : findTerms,
      getAllEnabledTables : getAllEnabledTables,
      getFamilies : getFamilies,
      copyTable : copyTable,
      getTableNamesForCluster : getTableNamesForCluster
    };
  }());

  var nodes = (function() {
    //------------- Private Methods -----------------
    function _getClusterData(clusterName) {
      var clusterData = $.grep(stateMap.nodeMap.clusters, function(cluster) {
        return cluster.name === clusterName;
      });
      return clusterData.length > 0 ? clusterData[0] : null;
    }

    //------------- Public API ----------------------
    function getOnlineZookeeperNodes() {
      return stateMap.nodeMap.zookeepers.online;
    }

    function getOfflineZookeeperNodes() {
      return stateMap.nodeMap.zookeepers.offline;
    }

    function getOnlineControllerNodes() {
      return stateMap.nodeMap.controllers.online;
    }

    function getOfflineControllerNodes() {
      return stateMap.nodeMap.controllers.offline;
    }

    function getOfflineShardNodes(clusterName) {
      var clusterData = _getClusterData(clusterName);
      return clusterData ? clusterData.offline : [];
    }

    function getOnlineShardNodes(clusterName) {
      var clusterData = _getClusterData(clusterName);
      return clusterData ? clusterData.online : [];
    }

    function isDataLoaded() {
      return stateMap.nodeMap !== null;
    }

    return {
      getOnlineZookeeperNodes : getOnlineZookeeperNodes,
      getOfflineZookeeperNodes : getOfflineZookeeperNodes,
      getOfflineControllerNodes : getOfflineControllerNodes,
      getOnlineControllerNodes : getOnlineControllerNodes,
      getOfflineShardNodes : getOfflineShardNodes,
      getOnlineShardNodes : getOnlineShardNodes,
      isDataLoaded : isDataLoaded
    };
  }());

  var metrics = (function() {
    //------------- Private Methods ------------------
    function _buildPieChartData(onlineCount, offlineCount) {
      var onlineChart = {
        'label':'Online',
        'color':'#66CDCC',
        'data':[[0,onlineCount]]
      };

      var offlineChart = {
        'label':'Offline',
        'color':'#FF1919',
        'data':[[0,offlineCount]]
      };

      return [onlineChart, offlineChart];
    }

    //------------- Public API -----------------------
    function getZookeeperChartData() {
      return _buildPieChartData(stateMap.nodeMap.zookeepers.online.length, stateMap.nodeMap.zookeepers.offline.length);
    }

    function getControllerChartData() {
      return _buildPieChartData(stateMap.nodeMap.controllers.online.length, stateMap.nodeMap.controllers.offline.length);
    }

    function getClusters() {
      return $.map(stateMap.nodeMap.clusters, function(cluster) {
        return cluster.name;
      });
    }

    function getShardChartData(clusterName) {
      var clusterData = $.grep(stateMap.nodeMap.clusters, function(cluster) {
        return cluster.name === clusterName;
      });

      if (clusterData.length > 0) {
        return _buildPieChartData(clusterData[0].online.length, clusterData[0].offline.length);
      }
      return null;
    }

    function getTableChartData() {
      var enabledData = blurconsole.utils.reduce(stateMap.currentTables, [], function(accumulator, table){
        var currentCluster = $.grep(accumulator, function(item){
          return item[0] === table.cluster;
        });

        if (currentCluster.length === 0) {
          currentCluster = [table.cluster, 0];
          accumulator.push(currentCluster);
        } else {
          currentCluster = currentCluster[0];
        }

        if (table.enabled) {
          currentCluster[1] = currentCluster[1]+1;
        }
        return accumulator;
      });

      var disabledData = blurconsole.utils.reduce(stateMap.currentTables, [], function(accumulator, table){
        var currentCluster = $.grep(accumulator, function(item){
          return item[0] === table.cluster;
        });

        if (currentCluster.length === 0) {
          currentCluster = [table.cluster, 0];
          accumulator.push(currentCluster);
        } else {
          currentCluster = currentCluster[0];
        }

        if (!table.enabled) {
          currentCluster[1] = currentCluster[1]+1;
        }
        return accumulator;
      });

      return [
        {
          'data' : enabledData,
          'label' : 'Enabled',
          'color' : '#66CDCC',
          'stack' : true
        },
        {
          'data' : disabledData,
          'label' : 'Disabled',
          'color' : '#333333',
          'stack' : true
        }
      ];
    }

    function getQueryLoadChartData() {
      var total = 0,
        queryArray = [],
        meanArray = [],
        queryData, mean;

      queryData = stateMap.queryPerformance;

      $.each(queryData, function(idx, increment) {
        total += increment;
      });

      mean = queryData.length === 0 ? 0 : total/queryData.length;

      $.each(queryData, function(idx, increment) {
        queryArray.push([idx, increment]);
        meanArray.push([idx, mean]);
      });

      return [{label: 'Queries', data: queryArray}, {label:'Average', data:meanArray}];
    }

    function getSlowQueryWarnings() {
      return stateMap.queries.slowQueries;
    }

    return {
      getZookeeperChartData : getZookeeperChartData,
      getControllerChartData : getControllerChartData,
      getClusters : getClusters,
      getShardChartData : getShardChartData,
      getTableChartData : getTableChartData,
      getQueryLoadChartData : getQueryLoadChartData,
      getSlowQueryWarnings : getSlowQueryWarnings
    };
  }());

  var queries = (function() {
    var states = ['running', 'interrupted', 'complete', 'backpressureinterrupted'];

    //-------------- Private Methods -------------------
    function _matchesFilter(queryData, filterText) {
      var queryStr = queryData.user + '~~~' + queryData.query + '~~~' + states[queryData.state];

      if (filterText === null || filterText === '') {
        return true;
      }

      return queryStr.toLowerCase().indexOf(filterText.toLowerCase()) !== -1;
    }

    //-------------- Public API -----------------------
    function queriesForTable(table, sort, filter) {
      var queries = [], qSort, sortField, sortDir;

      qSort = (sort || 'startTime~desc').split('~');
      sortField = qSort[0];
      sortDir = qSort.length > 1 ? qSort[1] : 'asc';

      $.each(stateMap.queries.queries, function(i, query){
        if (query.table === table && _matchesFilter(query, filter)) {
          queries.push(query);
        }
      });

      queries.sort(function(a, b){
        if (sortDir === 'asc') {
          return a[sortField] > b[sortField];
        } else {
          return b[sortField] > a[sortField];
        }
      });

      return queries;
    }

    function cancelQuery(table, uuid) {
      configMap.poller.cancelQuery(table, uuid);
    }

    function tableHasActivity(table) {
      var hasActivity = false;
      $.each(stateMap.queries.queries, function(i, query){
        if (query.table === table) {
          hasActivity = true;
          return false;
        }
      });
      return hasActivity;
    }

    return {
      queriesForTable : queriesForTable,
      cancelQuery : cancelQuery,
      tableHasActivity : tableHasActivity
    };
  }());

  var search = (function() {
    var results = {}, totalRecords = 0, currentQuery, currentTable, currentArgs = {start: 0, fetch: 10, rowRecordOption: 'rowrow', families: null};

    //-------------- Private Methods -------------------------
    function _sendSearch() {
      configMap.poller.sendSearch(currentQuery, currentTable, currentArgs, _processResults);
    }

    function _processResults(data) {
      var dataFamilies = data.families;
      var dataResults = data.results;
      totalRecords = data.total;

      if (typeof dataResults !== 'undefined' && dataResults !== null) {
        $.each(dataResults, function(family, resultList){
          var dataList = results[family] || [];
          results[family] = dataList.concat(resultList);
        });
      }
      $.gevent.publish('results-updated', [dataFamilies]);
    }

    //-------------- Public API ------------------------------
    function runSearch( query, table, searchArgs ) {
      var parsedFamilies = blurconsole.utils.findFamilies(query);

      currentQuery = query;
      currentTable = table;
      currentArgs = $.extend(currentArgs, searchArgs);
      if (parsedFamilies === null || parsedFamilies.length === 0) {
        var fams = tables.getFamilies(table);
        fams.sort();
        parsedFamilies = [fams[0]];
      }
      currentArgs.families = parsedFamilies;
      results = {};

      if (query.indexOf('rowid:') === -1 && query.indexOf('recordid:') === -1) {
        _sendSearch();
      }
    }

    function getResults() {
      return results;
    }

    function getTotal() {
      return totalRecords;
    }

    function loadMoreResults(family) {
      var alreadyLoadedResults = results[family];

      if (typeof alreadyLoadedResults === 'undefined' || alreadyLoadedResults === null) {
        currentArgs.start = 0;
      } else if ($.isArray(alreadyLoadedResults)) {
        currentArgs.start = alreadyLoadedResults.length;
      } else {
        currentArgs.start = blurconsole.utils.keys(alreadyLoadedResults).length;
      }

      // currentArgs.start = alreadyLoadedResults ? alreadyLoadedResults.length : 0;
      currentArgs.fetch = 10;
      currentArgs.families = [family];
      _sendSearch();
    }

    function runFacetCount( query, table, family, column, terms, callback ) {
      configMap.poller.runFacetCount(query, table, family, column, terms, callback);
    }

    return {
      runSearch: runSearch,
      getResults: getResults,
      loadMoreResults: loadMoreResults,
      getTotal: getTotal,
      runFacetCount: runFacetCount
    };
  }());

  var logs = (function() {
    //------------- Public API -------------------
    function logError(error, module) {
      stateMap.errors.push({error: error, module: module, timestamp: new Date()});
      $.gevent.publish('logging-updated');
    }

    function clearErrors() {
      delete stateMap.errors;
      stateMap.errors = [];
      $.gevent.publish('logging-updated');
    }

    function getLogs() {
      return stateMap.errors;
    }

    return {
      logError: logError,
      clearErrors: clearErrors,
      getLogs: getLogs
    };
  }());

  //----------------------- Private Methods -------------------------
  function _nodePoller() {
    configMap.poller.getNodeList(_updateNodes);
  }

  function _tablePoller() {
    configMap.poller.getTableList(_updateTables);
  }

  function _queryPerformancePoller() {
    configMap.poller.getQueryPerformance(_updateQueryPerformance);
  }

  function _queryPoller() {
    configMap.poller.getQueries(_updateQueries);
  }

  //----------------------- Event Handlers --------------------------
  function _updateNodes(nodes) {
    if (nodes !== 'error' && !blurconsole.utils.equals(nodes, stateMap.nodeMap)) {
      stateMap.nodeMap = nodes;
      $.gevent.publish('node-status-updated');
    }
    setTimeout(_nodePoller, blurconsole.config.refreshtime);
  }

  function _updateTables(data) {
    if (data !== 'error') {
      var tables = data.tables, clusters = data.clusters;
      if (!blurconsole.utils.equals(tables, stateMap.currentTables) || !blurconsole.utils.equals(clusters, stateMap.currentClusters)) {
        stateMap.currentTables = tables;
        stateMap.currentClusters = clusters;
        $.gevent.publish('tables-updated');
      }
    }
    setTimeout(_tablePoller, blurconsole.config.refreshtime);
  }

  function _updateQueryPerformance(performanceMetric) {
    if (performanceMetric !== 'error') {
      if (stateMap.queryPerformance.length === 100) {
        stateMap.queryPerformance.shift();
      }

      stateMap.queryPerformance.push(performanceMetric);
      $.gevent.publish('query-perf-updated');
    }
    setTimeout(_queryPerformancePoller, blurconsole.config.refreshtime);
  }

  function _updateQueries(queries) {
    if (queries !== 'error' && !blurconsole.utils.equals(queries, stateMap.queries)) {
      stateMap.queries.slowQueries = queries.slowQueries;
      // age current queries
      if(stateMap.queries.queries) {
        $.each(stateMap.queries.queries, function(idx, query) {
          query.age = (query.age || 0) + 1;
        });
      } else {
        stateMap.queries.queries = [];
      }
      // update queries
      $.each(queries.queries, function(idx, new_query){
        var found = false;
        $.each(stateMap.queries.queries, function(idx, old_query){
          if(old_query.uuid === new_query.uuid) {
            found = true;
            old_query.age = 0;
            old_query.state = new_query.state;
            old_query.percent = new_query.percent;
          }
        });
        if(!found) {
          stateMap.queries.queries.push(new_query);
        }
      });
      // remove old queries
      stateMap.queries.queries = $.grep(stateMap.queries.queries, function(query){
        return typeof query.age === 'undefined' || query.age < 5;
      });
      $.gevent.publish('queries-updated');
    }
    setTimeout(_queryPoller, blurconsole.config.refreshtime);
  }

  //----------------------- Public API ------------------------------
  function initModule() {
    if(window.location.href.indexOf('fakeIt=') > -1) {
      blurconsole.fake.initModule();
      configMap.poller = blurconsole.fake;
    } else {
      configMap.poller = blurconsole.data;
    }
    setTimeout(function() {
      _nodePoller();
      _tablePoller();
      _queryPerformancePoller();
      _queryPoller();
    }, 1000);
  }

  return {
    initModule : initModule,
    tables : tables,
    metrics: metrics,
    nodes : nodes,
    queries : queries,
    search : search,
    logs: logs
  };
}());