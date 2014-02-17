/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
								return '<div style="font-size:8pt;text-align:center;padding:2px;color:white">' + label + '<br/>' + Math.round(series.percent) + '% (' + series.data[0][1] + ')</div>';
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
			$container : null
		},
		jqueryMap = {},
		setJqueryMap, initModule, unloadModule, updateNodeCharts, adjustChartSize,
		loadZkPieChart,	loadControllerPieChart, loadShardsPieChart, loadTableColumnChart, loadQueryPerfLineChart;

	setJqueryMap = function() {
		var $container = stateMap.$container;
		jqueryMap = {
			$container : $container,
			$zkChartHolder : $('#zookeeperNodes'),
			$controllerChartHolder : $('#controllerNodes'),
			$shardChartHolder : $('#shardNodes'),
			$tableChartHolder : $('#tableCounts'),
			$queryLoadChartHolder : $('#queryLoad')
		};
	};

	unloadModule = function() {
		$.gevent.unsubscribe(jqueryMap.$container, 'node-status-updated');
		$.gevent.unsubscribe(jqueryMap.$container, 'tables-updated');
		$.gevent.unsubscribe(jqueryMap.$container, 'query-perf-updated');
	};

	updateNodeCharts = function() {
		loadZkPieChart();
		loadControllerPieChart();
		loadShardsPieChart();
	};

	loadZkPieChart = function() {
		$.plot(jqueryMap.$zkChartHolder, blurconsole.model.metrics.getZookeeperChartData(), configMap.pieOptions);
	};

	loadControllerPieChart = function() {
		$.plot(jqueryMap.$controllerChartHolder, blurconsole.model.metrics.getControllerChartData(), configMap.pieOptions);
	};

	loadShardsPieChart = function() {
		if (jqueryMap.$shardChartHolder.find('img').length > 0) {
			jqueryMap.$shardChartHolder.html('');
		}

		$.each(blurconsole.model.metrics.getClusters(), function(idx, cluster) {
			var clusterData, clusterHolder, parentSize;

			clusterData = blurconsole.model.metrics.getShardChartData(cluster);

			if (clusterData) {
				clusterHolder = jqueryMap.$shardChartHolder.find('#cluster_' + cluster + '_chart_holder');

				if (clusterHolder.length === 0) {
					clusterHolder = $('<div id="cluster_'+ cluster + '_chart_holder" class="shardClusterChartHolder simple-chart"></div>');
					jqueryMap.$shardChartHolder.append($('<div class="text-center"><strong>' + cluster + '</strong></div>'));
					jqueryMap.$shardChartHolder.append(clusterHolder);
					parentSize = jqueryMap.$shardChartHolder.parent()[0].clientWidth - 150;
					clusterHolder.css({
						'height' : parentSize,
						'width' : parentSize
					});
				}

				$.plot(clusterHolder, clusterData, configMap.pieOptions);
			}
		});
	};

	loadTableColumnChart = function() {
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
	};

	loadQueryPerfLineChart = function() {
		$.plot(jqueryMap.$queryLoadChartHolder, [blurconsole.model.metrics.getQueryLoadChartData()], {
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

	initModule = function( $container ) {
		$container.load ( configMap.view, function() {
			stateMap.$container = $container;
			setJqueryMap();
			$.gevent.subscribe(jqueryMap.$container, 'node-status-updated', updateNodeCharts);
			$.gevent.subscribe(jqueryMap.$container, 'tables-updated', loadTableColumnChart);
			$.gevent.subscribe(jqueryMap.$container, 'query-perf-updated', loadQueryPerfLineChart);
			adjustChartSize();
		});
		$(window).resize(adjustChartSize);
		return true;
	};

	return {
		initModule   : initModule,
		unloadModule : unloadModule
	};
}());