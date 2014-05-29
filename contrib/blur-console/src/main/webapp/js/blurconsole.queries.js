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
blurconsole.queries = (function () {
	'use strict';
	var configMap = {
		view : 'views/queries.tpl.html',
		states : ['Running', 'Interrupted', 'Complete', 'Back Pressure Interrupted'],
		queryDef : [
			{label:'User', key:'user'},
			{label:'Query', key: 'query'},
			{label:'Time Started', key: function(row){
				var start = new Date(row.startTime);
				return start.toTimeString(); //start.getHours() + ':' + start.getMinutes() + ':' + start.getSeconds();
			}},
			{label:'State', key: function(row) {
				var stateInfo = configMap.states[row.state];

				if (row.state === 0) {
					stateInfo += ' <div class="badge badge-info">' + row.percent + '%</div>';
				}
				return stateInfo;
			}},
			{label:'Actions', key: function(row) {
				var actions = '';
				if (row.state === 0) {
					actions += '<a href="#" class="cancelTrigger btn btn-danger" data-uuid="' + row.uuid + '" data-query="' + row.query + '" data-table="' + row.table + '"><i class="glyphicon glyphicon-ban-circle"></i> Cancel</a> ';
				}
				return actions;
			}}
		],
	},
	stateMap = {
		$container : null,
		currentTable : null,
		currentFilter : null,
		currentSort : null
	},
	jqueryMap = {},
	setJqueryMap, initModule, unloadModule, drawTableList, drawQueries, registerPageEvents, unregisterPageEvents, waitForData;

	setJqueryMap = function() {
		var $container = stateMap.$container;
		jqueryMap = {
			$container : $container,
			$tableHolder : $('#tableHolder'),
			$queryHolder : $('#queryHolder'),
			$filterHolder : $('#filterOptions'),
			$filterText : $('#filterOptions .filterText')
		};
	};

	registerPageEvents = function() {
		jqueryMap.$tableHolder.on('click', '.list-group-item', function(){
			stateMap.currentTable = $(this).attr('href');

			$('.list-group-item', jqueryMap.$tableHolder).removeClass('active');
			$('.list-group-item[href="' + stateMap.currentTable + '"]', jqueryMap.$tableHolder).addClass('active');
			drawQueries();
			return false;
		});
		jqueryMap.$queryHolder.on('click', 'a.cancelTrigger', function(){
			var uuid = $(this).data('uuid'), query = $(this).data('query'), table = $(this).data('table');
			var modalContent = blurconsole.browserUtils.modal('confirmDelete', 'Confirm Query Cancel', 'You are about to cancel the query [' + query + '].  Are you sure you want to do this?', [
				{classes: 'btn-primary killQuery', label: 'Stop Query'},
				{classes: 'btn-default cancel', label: 'Cancel', data: {dismiss:'modal'}}
			], 'medium');

			var modal = $(modalContent).modal().on('shown.bs.modal', function(e){
				$(e.currentTarget).on('click', '.killQuery', function() {
					blurconsole.model.queries.cancelQuery(table, uuid);
					modal.modal('hide');
				});
			}).on('hidden.bs.modal', function(e) {
				$(e.currentTarget).remove();
			});
			return false;
		});

		jqueryMap.$filterHolder.on('click', '.filterTrigger', function() {
			var filterVal = jqueryMap.$filterText.val();

			stateMap.currentFilter = filterVal;
			drawQueries();
		});
	};

	unregisterPageEvents = function() {
		if (jqueryMap.$tableHolder) {
			jqueryMap.$tableHolder.off();
		}
	};

	drawTableList = function() {
		var clusters = blurconsole.model.tables.getClusters();

		if (clusters) {
			jqueryMap.$tableHolder.html('');
			clusters.sort();
			$.each(clusters, function(i, cluster){
				var panelContent, tables = blurconsole.model.tables.getEnabledTables(cluster);

				panelContent =
					'<div class="panel panel-default">'
						+ '<div class="panel-heading">'
							+ '<h3 class="panel-title">' + cluster + '</h3>'
						+ '</div>'
						+ '<div class="panel-body">';
				if (tables.length > 0) {
					tables.sort(function(a, b){ return a.name > b.name; });
					panelContent += '<div class="list-group">';

					$.each(tables, function(i, table){
						panelContent += '<a href="' + table.name + '" class="list-group-item';
						if (table.name === stateMap.currentTable) {
							panelContent += ' active';
							drawQueries();
						}
						panelContent += '">' + table.name + '</a>';
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
	};

	drawQueries = function() {
		if (stateMap.currentTable){
			jqueryMap.$queryHolder.html(blurconsole.browserUtils.table(configMap.queryDef, blurconsole.model.queries.queriesForTable(stateMap.currentTable, stateMap.currentSort, stateMap.currentFilter)));
		} else {
			jqueryMap.$queryHolder.html('<div class="alert alert-info">Select a table on the left to see the current queries</div>');
		}
	};

	waitForData = function() {
		var clusters = blurconsole.model.tables.getClusters();
		if (clusters && clusters.length > 0) {
			drawTableList();
			drawQueries();
		} else {
			setTimeout(waitForData, 100);
		}
	};

	initModule = function($container) {
		$container.load(configMap.view, function() {
			stateMap.$container = $container;
			setJqueryMap();
			$.gevent.subscribe(jqueryMap.$container, 'queries-updated', drawQueries);
			$.gevent.subscribe(jqueryMap.$container, 'tables-updated', drawTableList);
			registerPageEvents();
			waitForData();
		});
		return true;
	};

	unloadModule = function() {
		$.gevent.unsubscribe(jqueryMap.$container, 'queries-updated');
		$.gevent.unsubscribe(jqueryMap.$container, 'tables-updated');
		unregisterPageEvents();
	};

	return {
		initModule : initModule,
		unloadModule : unloadModule
	};
}());