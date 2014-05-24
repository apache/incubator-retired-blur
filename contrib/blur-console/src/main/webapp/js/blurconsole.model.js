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
	var
		configMap = {
			poller : null
		},
		stateMap = {
			currentTables: null,
			currentClusters: [],
			nodeMap : null,
			queryPerformance : [],
			queries : {}
		},
		tables, metrics, nodes, queries, search, initModule, nodePoller, updateNodes, tablePoller, updateTables, queryPerformancePoller, updateQueryPerformance, queryPoller, updateQueries;

	tables = (function() {
		var getClusters, getEnabledTables, getDisabledTables, isDataLoaded, disableTable, enableTable, deleteTable, getSchema, findTerms, getAllEnabledTables, getFamilies;

		getClusters = function() {
			if (stateMap.currentClusters === null) {
				return [];
			}

			return blurconsole.utils.unique(stateMap.currentClusters, true);
		};

		getEnabledTables = function(cluster) {
			var data = [];

			$.each(stateMap.currentTables, function(idx, table) {
				if (table.cluster === cluster && table.enabled) {
					data.push({name:table.name, rowCount:table.rows, recordCount:table.records});
				}
			});

			return data;
		};

		getDisabledTables = function(cluster) {
			var data = [];

			$.each(stateMap.currentTables, function(idx, table) {
				if (table.cluster === cluster && !table.enabled) {
					data.push({name:table.name, rowCount:table.rows, recordCount:table.records});
				}
			});

			return data;
		};

		getAllEnabledTables = function() {
			var tableMap = {};

			$.each(getClusters(), function(c, cluster){
				tableMap[cluster] = getEnabledTables(cluster);
			});

			return tableMap;
		};

		isDataLoaded = function() {
			return stateMap.currentTables !== null;
		};

		disableTable = function(tableName) {
			configMap.poller.disableTable(tableName);
		};

		enableTable = function(tableName) {
			configMap.poller.enableTable(tableName);
		};

		deleteTable = function(tableName, includeFiles) {
			configMap.poller.deleteTable(tableName, includeFiles);
		};

		getSchema = function(tableName, callback) {
			configMap.poller.getSchema(tableName, callback);
		};

		getFamilies = function(tableName) {
			var table;

			$.each(stateMap.currentTables, function(idx, t) {
				if (t.name === tableName) {
					table = t;
					return false;
				}
			});

			return table.families;
		};

		findTerms = function(table, family, column, startsWith) {
			configMap.poller.findTerms(table, family, column, startsWith, function(terms) {
				$.gevent.publish('terms-updated', terms);
			});
		};

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
			getFamilies : getFamilies
		};
	}());

	nodes = (function() {
		var getOfflineZookeeperNodes, getOfflineControllerNodes, getOfflineShardNodes, isDataLoaded;

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

		isDataLoaded = function() {
			return stateMap.nodeMap !== null;
		};

		return {
			getOfflineZookeeperNodes : getOfflineZookeeperNodes,
			getOfflineControllerNodes : getOfflineControllerNodes,
			getOfflineShardNodes : getOfflineShardNodes,
			isDataLoaded : isDataLoaded
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

	queries = (function() {
		var queriesForTable, cancelQuery, tableHasActivity, matchesFilter,
			states = ['running', 'interrupted', 'complete', 'backpressureinterrupted'];

		queriesForTable = function(table, sort, filter) {
			var queries = [], qSort, sortField, sortDir;

			qSort = (sort || 'startTime~desc').split('~');
			sortField = qSort[0];
			sortDir = qSort.length > 1 ? qSort[1] : 'asc';

			$.each(stateMap.queries.queries, function(i, query){
				if (query.table === table && matchesFilter(query, filter)) {
					queries.push(query);
				}
			});

			queries.sort(function(a, b){
				if (sortDir === 'asc') {
					return a[sortField] > b[sortField];
				} else {
					return b[sortField] > b[sortField];
				}
			});

			return queries;
		};

		cancelQuery = function(table, uuid) {
			configMap.poller.cancelQuery(uuid);
		};

		tableHasActivity = function(table) {
			var hasActivity = false;
			$.each(stateMap.queries.queries, function(i, query){
				if (query.table === table) {
					hasActivity = true;
					return false;
				}
			});
			return hasActivity;
		};

		matchesFilter = function(queryData, filterText) {
			var queryStr = queryData.user + '~~~' + queryData.query + '~~~' + states[queryData.state];

			if (filterText === null || filterText === '') {
				return true;
			}

			return queryStr.toLowerCase().indexOf(filterText.toLowerCase()) !== -1;
		};

		return {
			queriesForTable : queriesForTable,
			cancelQuery : cancelQuery,
			tableHasActivity : tableHasActivity
		};
	}());

	search = (function() {
		var results = {}, totalRecords = 0, currentQuery, currentTable, currentArgs = {start: 0, fetch: 10, rowRecordOption: 'rowrow', families: null},
			runSearch, getResults, getFamilies, loadMoreResults, getTotal,
			sendSearch, processResults;

		runSearch = function( query, table, searchArgs ) {
			var parsedFamilies = blurconsole.utils.findFamilies(query);

			currentQuery = query;
			currentTable = table;
			currentArgs = $.extend(currentArgs, searchArgs);
			if (parsedFamilies.length > 0) {
				currentArgs.families = parsedFamilies;
			}
			results = {};
			sendSearch();
		};

		getResults = function() {
			return results;
		};

		getTotal = function() {
			return totalRecords;
		};

		loadMoreResults = function(family) {
			var alreadyLoadedResults = results[family];

			currentArgs.start = alreadyLoadedResults ? alreadyLoadedResults.length : 0;
			currentArgs.fetch = 10;
			currentArgs.families = [family];
			sendSearch();
		};

		sendSearch = function() {
			configMap.poller.sendSearch(currentQuery, currentTable, currentArgs, processResults);
		};

		processResults = function(data) {
			var dataFamilies, dataResults;

			dataFamilies = data.families;
			dataResults = data.results;
			totalRecords = data.total;

			if (typeof dataResults !== 'undefined' && dataResults !== null) {
				$.each(dataResults, function(family, resultList){
					var tmpList = results[family] || [];
					results[family] = tmpList.concat(resultList);
				});
			}
			$.gevent.publish('results-updated', [dataFamilies]);
		};

		return {
			runSearch: runSearch,
			getResults: getResults,
			getFamilies: getFamilies,
			loadMoreResults: loadMoreResults,
			getTotal: getTotal
		};
	}());

	nodePoller = function() {
		configMap.poller.getNodeList(updateNodes);
	};

	updateNodes = function(nodes) {
		if (!blurconsole.utils.equals(nodes, stateMap.nodeMap)) {
			stateMap.nodeMap = nodes;
			$.gevent.publish('node-status-updated');
		}
		setTimeout(nodePoller, 5000);
	};

	tablePoller = function() {
		configMap.poller.getTableList(updateTables);
	};

	updateTables = function(data) {
		var tables = data.tables, clusters = data.clusters;
		if (!blurconsole.utils.equals(tables, stateMap.currentTables) || !blurconsole.utils.equals(clusters, stateMap.currentClusters)) {
			stateMap.currentTables = tables;
			stateMap.currentClusters = clusters;
			$.gevent.publish('tables-updated');
		}
		setTimeout(tablePoller, 5000);
	};

	queryPerformancePoller = function() {
		configMap.poller.getQueryPerformance(updateQueryPerformance);
	};

	updateQueryPerformance = function(performanceMetric) {
		if (stateMap.queryPerformance.length === 100) {
			stateMap.queryPerformance.shift();
		}

		stateMap.queryPerformance.push(performanceMetric);
		$.gevent.publish('query-perf-updated');
		setTimeout(queryPerformancePoller, 5000);
	};

	queryPoller = function() {
		configMap.poller.getQueries(updateQueries);
	};

	updateQueries = function(queries) {
		if (!blurconsole.utils.equals(queries, stateMap.queries)) {
			stateMap.queries = queries;
			$.gevent.publish('queries-updated');
		}
		setTimeout(queryPoller, 5000);
	};

	initModule = function() {
		configMap.poller = window.location.href.indexOf('fakeIt=') > -1 ? blurconsole.fake : blurconsole.data;
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
		nodes : nodes,
		queries : queries,
		search : search
	};
}());