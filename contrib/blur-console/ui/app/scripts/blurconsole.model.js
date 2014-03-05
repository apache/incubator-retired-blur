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
/*global blurconsole:false */
blurconsole.model = (function() {
	'use strict';
	var
		configMap = {
			poller : null
		},
		stateMap = {
			tableNameMap: {},
			nodeMap : {},
			queryPerformance : [],
			queries : {}
		},
		isFakeData = true,
		tables, metrics, nodes, initModule, nodePoller, tablePoller, queryPerformancePoller, queryPoller;

	tables = (function() {
		var getClusters, getEnabledTables, getDisabledTables;

		getClusters = function() {
			return blurconsole.utils.unique($.map(stateMap.tableNameMap, function(table){
				return table.cluster;
			}), true);
		};

		getEnabledTables = function(cluster) {
			var data = [];

			$.each(stateMap.tableNameMap, function(idx, table) {
				if (table.cluster === cluster && table.enabled) {
					data.push({name:table.name, rowCount:table.rows, recordCount:table.records});
				}
			});

			return {
				cols : {
					name : {
						index : 1,
						type : 'string'
					},
					rowCount : {
						index : 2,
						type : 'number'
					},
					recordCount : {
						index : 3,
						type : 'number'
					}
				},
				rows: data
			};
		};

		getDisabledTables = function(cluster) {
			console.log(cluster);
		};

		return {
			getClusters : getClusters,
			getEnabledTables : getEnabledTables,
			getDisabledTables : getDisabledTables
		};
	}());

	nodes = (function() {
		var getOfflineZookeeperNodes, getOfflineControllerNodes, getOfflineShardNodes;

		getOfflineZookeeperNodes = function() {
			return stateMap.nodeMap.zookeepers.offline;
		};

		getOfflineControllerNodes = function() {
			return stateMap.nodeMap.controllers.offline;
		};

		getOfflineShardNodes = function(clusterName) {
			var clusterData = $.grep(stateMap.nodeMap.clusters, function(cluster) {
				return cluster.name === clusterName;
			});

			if (clusterData.length > 0) {
				return clusterData[0].offline;
			}
			return [];
		};

		return {
			getOfflineZookeeperNodes : getOfflineZookeeperNodes,
			getOfflineControllerNodes : getOfflineControllerNodes,
			getOfflineShardNodes : getOfflineShardNodes
		};
	}());

	metrics = (function() {
		var getZookeeperChartData, getControllerChartData, getClusters, getShardChartData, getTableChartData,
			getQueryLoadChartData, buildPieChartData, getSlowQueryWarnings;

		getZookeeperChartData = function() {
			return buildPieChartData(stateMap.nodeMap.zookeepers.online.length, stateMap.nodeMap.zookeepers.offline.length);
		};

		getControllerChartData = function() {
			return buildPieChartData(stateMap.nodeMap.controllers.online.length, stateMap.nodeMap.controllers.offline.length);
		};

		getClusters = function() {
			return $.map(stateMap.nodeMap.clusters, function(cluster) {
				return cluster.name;
			});
		};

		getShardChartData = function(clusterName) {
			var clusterData = $.grep(stateMap.nodeMap.clusters, function(cluster) {
				return cluster.name === clusterName;
			});

			if (clusterData.length > 0) {
				return buildPieChartData(clusterData[0].online.length, clusterData[0].offline.length);
			}
			return null;
		};

		getTableChartData = function() {
			var enabledData = blurconsole.utils.reduce(stateMap.tableNameMap, [], function(accumulator, table){
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

			var disabledData = blurconsole.utils.reduce(stateMap.tableNameMap, [], function(accumulator, table){
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
		};

		getQueryLoadChartData = function() {
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

			return [queryArray, meanArray];
		};

		buildPieChartData = function(onlineCount, offlineCount) {
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
		};

		getSlowQueryWarnings = function() {
			return stateMap.queries.slowQueries;
		};

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

	nodePoller = function() {
		stateMap.nodeMap = configMap.poller.getNodeList();
		$.gevent.publish('node-status-updated');
		setTimeout(nodePoller, 5000);
	};

	tablePoller = function() {
		stateMap.tableNameMap = configMap.poller.getTableList();
		$.gevent.publish('tables-updated');
		setTimeout(tablePoller, 5000);
	};

	queryPerformancePoller = function() {
		if (stateMap.queryPerformance.length === 100) {
			stateMap.queryPerformance.shift();
		}

		stateMap.queryPerformance.push(configMap.poller.getQueryPerformance());
		$.gevent.publish('query-perf-updated');
		setTimeout(queryPerformancePoller, 1000);
	};

	queryPoller = function() {
		stateMap.queries = configMap.poller.getQueries();
		$.gevent.publish('queries-updated');
		setTimeout(queryPoller, 5000);
	};

	initModule = function() {
		configMap.poller = isFakeData ? blurconsole.fake : blurconsole.data;
		setTimeout(function() {
			nodePoller();
			tablePoller();
			queryPerformancePoller();
			queryPoller();
		}, 1000);
	};
	return {
		initModule : initModule,
		tables : tables,
		metrics: metrics,
		nodes : nodes
	};
}());