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
blurconsole.search = (function () {
	'use strict';
	var configMap = {
		view : 'views/search.tpl.html'
	},
	stateMap = {
		$container : null,
		$currentTable : null,
		$currentQuery : null,
		$schemaForCurrentTable : null,
		$start : 0,
		$fetch : 10,
		$filter : null,
		$rowRecordOption : 'rowrow'
	},
	jqueryMap = {},
	setJqueryMap, initModule, unloadModule, drawResults, registerPageEvents, unregisterPageEvents, sendSearch, showOptions, reviewTables, loadTableList;

	setJqueryMap = function() {
		var $container = stateMap.$container;
		jqueryMap = {
			$container : $container,
			$queryField : $('#queryField'),
			$tableField : $('#tableChooser'),
			$tableSelectorStatusOption : $('#statusOption'),
			$tableWarning : $('#tableGoneWarning')
		};
	};

	registerPageEvents = function() {
		$('#searchTrigger').on('click', sendSearch);
		$('#searchOptionsTrigger').on('click', showOptions);
	};

	unregisterPageEvents = function() {
		$('#searchTrigger').off('click');
		$('#searchOptionsTrigger').off('click');
	};

	sendSearch = function() {
		// Save options
		stateMap.currentTable = jqueryMap.$tableField.val();
		stateMap.currentQuery = jqueryMap.$queryField.val();

		blurconsole.shell.changeAnchorPart({
			tab: 'search',
			_tab: {
				query: encodeURIComponent(stateMap.currentQuery),
				table: stateMap.currentTable
			}
		});

		blurconsole.model.search.runSearch(stateMap.currentQuery, stateMap.currentTable, {start: 0, fetch: 10});
	};

	showOptions = function() {

	};

	reviewTables = function() {
		var tableFound = false, tableMap;

		if (stateMap.currentTable) {
			tableMap = blurconsole.model.tables.getAllEnabledTables();
			$.each(tableMap, function(cluster, tables){
				var tableList = $.map(tables, function(t){ return t.name; });
				if (tableList.indexOf(stateMap.currentTable) > -1) {
					tableFound = true;
				}
			});
		}

		if (tableFound) {
			jqueryMap.$tableWarning.hide();
			loadTableList();
		} else {
			jqueryMap.$tableWarning.show();
		}
	};

	drawResults = function() {

	};

	loadTableList = function() {
		var tableMap = blurconsole.model.tables.getAllEnabledTables();

		jqueryMap.$tableSelectorStatusOption.html('Loading Tables...');
		jqueryMap.$tableField.find('optgroup').remove();

		$.each(tableMap, function(cluster, tables) {
			var optGroupString;

			optGroupString = '<optgroup label="' + cluster + '">';
			$.each(tables, function(t, table){
				optGroupString += '<option value="' + table.name + '"' + (table.name === stateMap.currentTable ? ' selected' : '') + '>' + table.name + '</option>';
			});
			optGroupString += '</optgroup>';
			jqueryMap.$tableField.append(optGroupString);
		});

		jqueryMap.$tableSelectorStatusOption.html('Choose Table');
	};

	initModule = function($container) {
		$container.load(configMap.view, function() {
			stateMap.$container = $container;
			setJqueryMap();
			$.gevent.subscribe(jqueryMap.$container, 'tables-updated', reviewTables);
			registerPageEvents();
			loadTableList();

			var startupMap = $.uriAnchor.makeAnchorMap();

			stateMap.currentQuery = startupMap._tab.query;
			jqueryMap.$queryField.val(stateMap.currentQuery);
			stateMap.currentTable = startupMap._tab.table;
			jqueryMap.$tableField.val(stateMap.currentTable);

		});
		return true;
	};

	unloadModule = function() {
		$.gevent.unsubscribe(jqueryMap.$container, 'tables-updated');
		unregisterPageEvents();
	};

	return {
		initModule : initModule,
		unloadModule : unloadModule
	};
}());