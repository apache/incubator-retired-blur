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
blurconsole.tables = (function () {
	'use strict';
	var configMap = {
		view : 'views/tables.tpl.html',
		enabledDef : [
			{label:'Table Name', key:'name'},
			{label:'Row Count', key: 'rowCount'},
			{label:'Record Count', key: 'recordCount'},
			{label:'Actions', key: function(row) {
				var actions = '', table = row.name;
				actions += '<a href="#" class="schemaTrigger btn btn-default" data-name="' + table + '"><i class="glyphicon glyphicon-list-alt"></i> Schema</a> ';
				actions += '<a href="#" class="disableTrigger btn btn-danger" data-name="' + table + '"><i class="glyphicon glyphicon-cloud-download"></i> Disable</a> ';
				return actions;
			}}
		],
		disabledDef : [
			{label:'Table Name', key:'name'},
			{label:'Actions', key: function(row) {
				var actions = '', table = row.name;
				actions += '<a href="#" class="enableTrigger btn btn-default" data-name="' + table + '"><i class="glyphicon glyphicon-cloud-upload"></i> Enable</a> ';
				actions += '<a href="#" class="deleteTrigger btn btn-danger" data-name="' + table + '"><i class="glyphicon glyphicon-trash"></i> Delete</a> ';
				return actions;
			}}
		]
	},
	stateMap = { $container : null },
	jqueryMap = {},
	setJqueryMap, initModule, unloadModule, updateTableList, buildTabs, waitForData, registerPageEvents, unregisterPageEvents;

	setJqueryMap = function() {
		var $container = stateMap.$container;
		jqueryMap = {
			$container : $container,
			$tableInfoHolder : $('#tableInfoHolder'),
			$tables : {}
		};
	};

	unloadModule = function() {
		$.gevent.unsubscribe(jqueryMap.$container, 'tables-updated');
		unregisterPageEvents();
	};

	initModule = function($container) {
		$container.load(configMap.view, function() {
			stateMap.$container = $container;
			setJqueryMap();
			$.gevent.subscribe(jqueryMap.$container, 'tables-updated', updateTableList);
			waitForData();
			registerPageEvents();
		});
		return true;
	};

	waitForData = function() {
		if (blurconsole.model.tables.getClusters().length > 0) {
			buildTabs();
		} else {
			setTimeout(waitForData, 100);
		}
	};

	buildTabs = function() {
		var clusters, tabMarkup, paneMarkup, needsTabs;

		clusters = blurconsole.model.tables.getClusters();
		needsTabs = clusters.length > 1;

		if (needsTabs) {
			tabMarkup = '<ul class="nav nav-tabs">';
			tabMarkup += $.map(clusters, function(cluster, idx) {
				return '<li class="' + (idx === 0 ? 'active' : '') + '"><a href="#' + cluster + '_pane" data-toggle="tab">' + cluster + '</a></li>';
			}).join('');
			tabMarkup += '</ul>';

			jqueryMap.$tableInfoHolder.html($(tabMarkup));
		}

		paneMarkup = needsTabs ? '<div class="tab-content">' : '';
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
	};

	updateTableList = function() {
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
	};

	registerPageEvents = function() {
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
			console.log('delete:' + tableName);
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
	};

	unregisterPageEvents = function() {
		if (jqueryMap.$tableInfoHolder) {
			jqueryMap.$tableInfoHolder.off();
		}
	};

	return {
		initModule : initModule,
		unloadModule : unloadModule
	};
}());