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

/*
 * blurconsole.dashboard.js
 * Dashboard feature module for Blur Console
 */
/*global blurconsole:false */
blurconsole.dashboard = (function () {
  'use strict';
  //------------------- Configuration and State -------------------------
  var
    configMap = {
      view: 'views/dashboard.tpl.html',
      pieOptions : {
        series : {
          pie : {
            show : true,
            radius : 1,
            label : {
              show : true,
              radius : 2/3,
              formatter : function(label, series) {
                return '<div style="font-size:10pt;font-weight:bold;text-align:center;padding:2px;color:white">' + label + '<br/>' + Math.round(series.percent) + '% (' + series.data[0][1] + ')</div>';
              },
              threshold : 0.1
            }
          }
        },
        legend : {
          show : false
        },
        title: 'Test title'
      }
    },
    stateMap = {
      $container : null,
      zookeeperNodes: 'chart',
      controllerNodes: 'chart'
    },
    jqueryMap = {};

  //--------------------- Utilities ---------------------
  function _setJqueryMap() {
    var $container = stateMap.$container;
    jqueryMap = {
      $container : $container,
      $zkChartHolder : $('#zookeeperNodes'),
      $zkInfoHolder : $('#zookeeperInfo'),
      $controllerChartHolder : $('#controllerNodes'),
      $controllerInfoHolder : $('#controllerInfo'),
      $shardChartHolder : $('#shardNodes'),
      $tableChartHolder : $('#tableCounts'),
      $queryLoadChartHolder : $('#queryLoad')
    };
  }

  function _registerPageEvents() {
    $(document).on('click', '.swapper-trigger', _swapNodeChartAndData);
    $(window).on('resize', _adjustChartsSizes);
  }

  function _unregisterPageEvents() {
    $(document).off('click', '.swapper-trigger');
    $(window).off('resize');
  }

  //----------------------- Event Handlers and DOM Modifiers ------------------
  function _updateAllCharts() {
    _updateNodeCharts();
    _loadTableColumnChart();
    _loadQueryPerfLineChart();
  }

  function _updateNodeCharts() {
    if (blurconsole.model.nodes.isDataLoaded()) {
      _loadZkPieChart();
      _loadControllerPieChart();
      _loadShardsPieChart();
    }
  }

  function _swapNodeChartAndData(evt) {
    var parent = $(evt.currentTarget).closest('div.swapper-parent');
    var chart = parent.find('.swapper-chart');
    var info = parent.find('.swapper-info');

    chart.toggleClass('hidden');
    info.toggleClass('hidden');
  }

  function _loadZkPieChart() {
    $.plot(jqueryMap.$zkChartHolder, blurconsole.model.metrics.getZookeeperChartData(), configMap.pieOptions);
    jqueryMap.$zkInfoHolder.html(_buildNodeTable(blurconsole.model.nodes.getOnlineZookeeperNodes(), blurconsole.model.nodes.getOfflineZookeeperNodes()));
  }

  function _loadControllerPieChart() {
    $.plot(jqueryMap.$controllerChartHolder, blurconsole.model.metrics.getControllerChartData(), configMap.pieOptions);
    jqueryMap.$controllerInfoHolder.html(_buildNodeTable(blurconsole.model.nodes.getOnlineControllerNodes(), blurconsole.model.nodes.getOfflineControllerNodes()));
  }

  function _loadShardsPieChart() {
    if (jqueryMap.$shardChartHolder.find('img').length > 0) {
      jqueryMap.$shardChartHolder.html('');
    }

    $.each(blurconsole.model.metrics.getClusters(), function(idx, cluster) {
      var clusterData = blurconsole.model.metrics.getShardChartData(cluster);

      if (clusterData) {
        var clusterHolder = jqueryMap.$shardChartHolder.find('#cluster_' + cluster + '_chart_holder');
        var clusterInfo = jqueryMap.$shardChartHolder.find('#cluster_' + cluster + '_info');

        if (clusterHolder.length === 0) {
          var wrapper = $('<div class="swapper-parent"></div>');
          wrapper.append($('<div class="text-center"><strong>' + cluster + '</strong> <small class="text-muted"><i class="glyphicon glyphicon-retweet swapper-trigger" title="Swap Chart/Info"></i></small></div>'));
          clusterHolder = $('<div id="cluster_'+ cluster + '_chart_holder" class="shardClusterChartHolder simple-chart swapper-chart"></div>');
          wrapper.append(clusterHolder);
          var parentSize = jqueryMap.$shardChartHolder.parent()[0].clientWidth - 150;
          clusterHolder.css({
            'height' : parentSize,
            'width' : parentSize
          });
          clusterInfo = $('<div id="cluster_' + cluster + '_info" class="swapper-info hidden"></div>');
          wrapper.append(clusterInfo);

          jqueryMap.$shardChartHolder.append(wrapper);
        }

        $.plot(clusterHolder, clusterData, configMap.pieOptions);
        clusterInfo.html(_buildNodeTable([], blurconsole.model.nodes.getOfflineShardNodes(cluster)));
      }
    });
  }

  function _loadTableColumnChart() {
    if (blurconsole.model.tables.isDataLoaded()) {
      $.plot(jqueryMap.$tableChartHolder, blurconsole.model.metrics.getTableChartData(), {
        bars : {
          show : true,
          barWidth : 0.6,
          align : 'center'
        },
        yaxis : {
          min : 0,
          tickDecimals : 0
        },
        xaxis : {
          mode : 'categories'
        }
      });
    }
  }

  function _loadQueryPerfLineChart() {
    $.plot(jqueryMap.$queryLoadChartHolder, blurconsole.model.metrics.getQueryLoadChartData(), {
      series : {
        shadowSize : 0
      },
      yaxis : {
        min : 0
      },
      xaxis : {
        show : false
      }
    });
  }

  function _buildNodeTable(online, offline) {
    var table = '<table class="table table-condensed"><thead><tr><th>Offline Node</th></tr></thead><tbody>';
    if (offline.length === 0) {
      table += '<tr><td>Everything is Online!</td></tr>';
    } else {
      $.each(offline, function(idx, node) {
        table += '<tr><td>' + node + '</td></tr>';
      });
    }
    table += '</tbody></table>';

    if (online.length > 0) {
      table += '<table class="table table-condensed"><thead><tr><th>Online Node</th></tr></thead><tbody>';
      $.each(online, function(idx, node) {
        table += '<tr><td>' + node + '</td></tr>';
      });
      table += '</tbody></table>';
    }
    return $(table);
  }

  function _adjustChartSize(holder) {
    var size;
    size = jqueryMap[holder].parent()[0].clientWidth - 150;
    jqueryMap[holder].css({
      'height' : size,
      'width' : size
    });
  }

  function _adjustChartsSizes() {
    _adjustChartSize('$zkChartHolder');
    _adjustChartSize('$controllerChartHolder');
    _adjustChartSize('$tableChartHolder');
    _adjustChartSize('$queryLoadChartHolder');

    var size = jqueryMap.$shardChartHolder.parent()[0].clientWidth - 150;
    var shardCharts = jqueryMap.$shardChartHolder.find('.shardClusterChartHolder');
    shardCharts.each(function(){
      $(this).css({
        'height' : size,
        'width' : size
      });
    });
    _updateAllCharts();
  }

  function _checkForSlowQueries() {
    if (blurconsole.model.metrics.getSlowQueryWarnings()) {
      $('#slow-query-warnings').slideDown();
    } else {
      $('#slow-query-warnings').slideUp();
    }
  }

  //----------------------- Public API ----------------------------

  function initModule( $container ) {
    $container.load ( configMap.view, function() {
      stateMap.$container = $container;
      _setJqueryMap();
      _updateAllCharts();
      _checkForSlowQueries();
      $.gevent.subscribe(jqueryMap.$container, 'node-status-updated', _updateNodeCharts);
      $.gevent.subscribe(jqueryMap.$container, 'tables-updated', _loadTableColumnChart);
      $.gevent.subscribe(jqueryMap.$container, 'query-perf-updated', _loadQueryPerfLineChart);
      $.gevent.subscribe(jqueryMap.$container, 'queries-updated', _checkForSlowQueries);
      _adjustChartsSizes();
      _registerPageEvents();
    });
    return true;
  }

  function unloadModule() {
    $.gevent.unsubscribe(jqueryMap.$container, 'node-status-updated');
    $.gevent.unsubscribe(jqueryMap.$container, 'tables-updated');
    $.gevent.unsubscribe(jqueryMap.$container, 'query-perf-updated');
    $.gevent.unsubscribe(jqueryMap.$container, 'queries-updated');
    _unregisterPageEvents();
  }

  return {
    initModule   : initModule,
    unloadModule : unloadModule
  };
}());