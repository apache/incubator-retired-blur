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
		jqueryMap = {},
		setJqueryMap, initModule, unloadModule, updateNodeCharts, adjustChartSize,
		loadZkPieChart,	loadControllerPieChart, loadShardsPieChart, loadTableColumnChart, loadQueryPerfLineChart,
		buildNodeTable, checkForSlowQueries;

	setJqueryMap = function() {
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
	};

	unloadModule = function() {
		$.gevent.unsubscribe(jqueryMap.$container, 'node-status-updated');
		$.gevent.unsubscribe(jqueryMap.$container, 'tables-updated');
		$.gevent.unsubscribe(jqueryMap.$container, 'query-perf-updated');
		$.gevent.unsubscribe(jqueryMap.$container, 'queries-updated');
	};

	updateNodeCharts = function() {
		if (blurconsole.model.nodes.isDataLoaded()) {
			loadZkPieChart();
			loadControllerPieChart();
			loadShardsPieChart();
		}
	};

	loadZkPieChart = function() {
		$.plot(jqueryMap.$zkChartHolder, blurconsole.model.metrics.getZookeeperChartData(), configMap.pieOptions);
		jqueryMap.$zkInfoHolder.html(buildNodeTable(blurconsole.model.nodes.getOfflineZookeeperNodes()));
	};

	loadControllerPieChart = function() {
		$.plot(jqueryMap.$controllerChartHolder, blurconsole.model.metrics.getControllerChartData(), configMap.pieOptions);
		jqueryMap.$controllerInfoHolder.html(buildNodeTable(blurconsole.model.nodes.getOfflineControllerNodes()));
	};

	loadShardsPieChart = function() {
		if (jqueryMap.$shardChartHolder.find('img').length > 0) {
			jqueryMap.$shardChartHolder.html('');
		}

		$.each(blurconsole.model.metrics.getClusters(), function(idx, cluster) {
			var clusterData, clusterHolder, clusterInfo, parentSize;

			clusterData = blurconsole.model.metrics.getShardChartData(cluster);

			if (clusterData) {
				clusterHolder = jqueryMap.$shardChartHolder.find('#cluster_' + cluster + '_chart_holder');
				clusterInfo = jqueryMap.$shardChartHolder.find('#cluster_' + cluster + '_info');

				if (clusterHolder.length === 0) {
					var wrapper = $('<div class="swapper-parent"></div>');
					wrapper.append($('<div class="text-center"><strong>' + cluster + '</strong> <small class="text-muted"><i class="glyphicon glyphicon-retweet swapper-trigger" title="Swap Chart/Info"></i></small></div>'));
					clusterHolder = $('<div id="cluster_'+ cluster + '_chart_holder" class="shardClusterChartHolder simple-chart swapper-chart"></div>');
					wrapper.append(clusterHolder);
					parentSize = jqueryMap.$shardChartHolder.parent()[0].clientWidth - 150;
					clusterHolder.css({
						'height' : parentSize,
						'width' : parentSize
					});
					clusterInfo = $('<div id="cluster_' + cluster + '_info" class="swapper-info hidden"></div>');
					wrapper.append(clusterInfo);

					jqueryMap.$shardChartHolder.append(wrapper);
				}

				$.plot(clusterHolder, clusterData, configMap.pieOptions);
				clusterInfo.html(buildNodeTable(blurconsole.model.nodes.getOfflineShardNodes(cluster)));
			}
		});
	};

	loadTableColumnChart = function() {
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
	};

	loadQueryPerfLineChart = function() {
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
	};

	buildNodeTable = function(data) {
		var table = '<table class="table table-condensed"><thead><tr><th>Offline Node</th></tr></thead><tbody>';
		if (data.length === 0) {
			table += '<tr><td>Everything is Online!</td></tr>';
		} else {
			$.each(data, function(idx, node) {
				table += '<tr><td>' + node + '</td></tr>';
			});
		}
		table += '</tbody></table>';
		return $(table);
	};

	adjustChartSize = function() {
		var size, shardCharts;

		size = jqueryMap.$zkChartHolder.parent()[0].clientWidth - 150;
		jqueryMap.$zkChartHolder.css({
			'height' : size,
			'width' : size
		});

		size = jqueryMap.$controllerChartHolder.parent()[0].clientWidth - 150;
		jqueryMap.$controllerChartHolder.css({
			'height' : size,
			'width' : size
		});

		size = jqueryMap.$shardChartHolder.parent()[0].clientWidth - 150;
		shardCharts = jqueryMap.$shardChartHolder.find('.shardClusterChartHolder');
		shardCharts.each(function(){
			$(this).css({
				'height' : size,
				'width' : size
			});
		});

		size = jqueryMap.$tableChartHolder.parent()[0].clientWidth - 150;
		jqueryMap.$tableChartHolder.css({
			'height' : size,
			'width' : size
		});

		size = jqueryMap.$queryLoadChartHolder.parent()[0].clientWidth - 150;
		jqueryMap.$queryLoadChartHolder.css({
			'height' : size,
			'width' : size
		});
	};

	checkForSlowQueries = function() {
		if (blurconsole.model.metrics.getSlowQueryWarnings()) {
			$('#slow-query-warnings').removeClass('hidden');
		} else {
			$('#slow-query-warnings').addClass('hidden');
		}
	};

	initModule = function( $container ) {
		$container.load ( configMap.view, function() {
			stateMap.$container = $container;
			setJqueryMap();
			updateNodeCharts();
			loadTableColumnChart();
			loadQueryPerfLineChart();
			checkForSlowQueries();
			$.gevent.subscribe(jqueryMap.$container, 'node-status-updated', updateNodeCharts);
			$.gevent.subscribe(jqueryMap.$container, 'tables-updated', loadTableColumnChart);
			$.gevent.subscribe(jqueryMap.$container, 'query-perf-updated', loadQueryPerfLineChart);
			$.gevent.subscribe(jqueryMap.$container, 'queries-updated', checkForSlowQueries);
			adjustChartSize();
			$(document).on('click', '.swapper-trigger', function() {
			    console.log(this);
				var parent = $(this).closest('div.swapper-parent');
				console.log(parent);
				var chart = parent.find('.swapper-chart');
				var info = parent.find('.swapper-info');

				chart.toggleClass('hidden');
				info.toggleClass('hidden');
			});
			
		});
		$(window).resize(adjustChartSize);
		return true;
	};

	return {
		initModule   : initModule,
		unloadModule : unloadModule
	};
}());