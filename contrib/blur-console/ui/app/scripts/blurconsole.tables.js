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
		tableSettings : {
			pageSize : 500,
			pageSizes : null
		}
	},
	stateMap = { $container : null },
	jqueryMap = {},
	setJqueryMap, initModule, unloadModule, updateTableList, buildTabs, waitForData;

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
	};

	initModule = function($container) {
		$container.load(configMap.view, function() {
			stateMap.$container = $container;
			setJqueryMap();
			$.gevent.subscribe(jqueryMap.$container, 'tables-updated', updateTableList);
			waitForData();
			jqueryMap.$tableInfoHolder.on('click', 'ul.nav a', function(e) {
				e.preventDefault();
				$(this).tab('show');
			});
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
			var clusterPane = $('#' + cluster + '_pane'), enabledSection, disabledSection;
			clusterPane.find('.enabledSection').WATable({

			});
			clusterPane.find('.disabledSection').WATable();
		});
	};

	updateTableList = function() {
		console.log('Updating tables');
		var clusters = blurconsole.model.tables.getClusters();

		$.each(clusters, function(idx, cluster) {
			var clusterPane = $('#' + cluster + '_pane'), enabledSection, disabledSection;
			enabledSection = clusterPane.find('.enabledSection');
			disabledSection = clusterPane.find('.disabledSection');

			if (enabledSection.length > 0) {
				console.log(blurconsole.model.tables.getEnabledTables(cluster));
				console.log(enabledSection);
				enabledSection.data('WATable').setData(blurconsole.model.tables.getEnabledTables(cluster), false, false);
			}
			if (disabledSection.length > 0) {
				disabledSection.data('WATable').setData(blurconsole.model.tables.getDisabledTables(cluster), false, false);
			}
		});
	};

	return {
		initModule : initModule,
		unloadModule : unloadModule
	};
}());