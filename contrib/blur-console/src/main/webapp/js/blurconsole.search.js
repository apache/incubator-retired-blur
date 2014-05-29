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
/*global blurconsole:false, confirm:false */
blurconsole.search = (function () {
	'use strict';
	var configMap = {
		view : 'views/search.tpl.html',
		superQueryMap: {
			'rowrow' : 'Search Row / Retrieve Row',
			'recordrow' : 'Search Record / Retrieve Row',
			'recordrecord' : 'Search Record / Retrieve Record'
		},
		optionsHtml:
			'<label for="superQuery">Search & Retrieve</label>' +
			'<select id="superQuery">' +
				'<option value="rowrow">Search Row / Retrieve Row</option>' +
				'<option value="recordrow">Search Record / Retrieve Row</option>' +
				'<option value="recordrecord">Search Record / Retrieve Record</option>' +
			'</select>'
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
	setJqueryMap, initModule, unloadModule, drawResultHolders, drawResults, registerPageEvents, unregisterPageEvents,
	sendSearch, showOptions, reviewTables, loadTableList, getMoreData, fixPanelWidths, updateOptionPopover, updateOptionDisplay,
	persistOptions, getColList, popupFacetDialog;

	setJqueryMap = function() {
		var $container = stateMap.$container;
		jqueryMap = {
			$container : $container,
			$queryField : $('#queryField'),
			$tableField : $('#tableChooser'),
			$tableSelectorStatusOption : $('#statusOption'),
			$tableWarning : $('#tableGoneWarning'),
			$resultsHolder : $('#results'),
			$optionsDisplay : $('#searchOptionsDisplay'),
			$countHolder : $('#resultCount'),
			$facetTrigger : $('#facetTrigger')
		};
	};

	registerPageEvents = function() {
		$('#searchTrigger').on('click', sendSearch);
		$('#results').on('shown.bs.collapse', '.panel-collapse:not(.loaded)', getMoreData);
		$('#results').on('click', '.nextPage', getMoreData);
		$('#searchOptionsTrigger').popover({
			html: true,
			placement: 'bottom',
			title: 'Extra Search Options',
			container: 'body',
			content: configMap.optionsHtml
		});
		$('#searchOptionsTrigger').on('shown.bs.popover', updateOptionPopover);
		$(document).on('change', '.popover select', persistOptions);
		jqueryMap.$facetTrigger.on('click', popupFacetDialog);
	};

	unregisterPageEvents = function() {
		$('#searchTrigger').off('click');
		$('#results').off('shown.bs.collapse');
		$('#results').off('click');
		$('#searchOptionsTrigger').popover('destroy');
		$('#searchOptionsTrigger').off('shown.bs.popover');
		$(document).off('change');
		//jqueryMap.$facetTrigger.off('click');
	};

	updateOptionDisplay = function() {
		var displayText = '';

		displayText += configMap.superQueryMap[stateMap.$rowRecordOption];

		jqueryMap.$optionsDisplay.html(displayText);
	};

	updateOptionPopover = function() {
		if ($('#superQuery').length > 0) {
			$('#superQuery').val(stateMap.$rowRecordOption);
		}
	};

	persistOptions = function() {
		var resendSearch = false;
		if (jqueryMap.$resultsHolder.children().length > 0) {
			if (confirm('You have existing results on the screen, changing the search options will erase your results.  Continue?')) {
				resendSearch = true;
			} else {
				$('#superQuery').val(stateMap.$rowRecordOption);
				return false;
			}
		}
		stateMap.$rowRecordOption = $('#superQuery').val();
		if (resendSearch) {
			sendSearch();
		}
		updateOptionDisplay();
		$('#searchOptionsTrigger').popover('hide');
	};

	sendSearch = function() {
		stateMap.$currentTable = jqueryMap.$tableField.val();
		stateMap.$currentQuery = jqueryMap.$queryField.val();

		blurconsole.shell.changeAnchorPart({
			tab: 'search',
			_tab: {
				query: encodeURIComponent(stateMap.$currentQuery),
				table: stateMap.$currentTable,
				rr: stateMap.$rowRecordOption
			}
		});

		drawResultHolders();

		blurconsole.model.search.runSearch(stateMap.$currentQuery, stateMap.$currentTable, {start: 0, fetch: 10, rowRecordOption: stateMap.$rowRecordOption});
	};

	getMoreData = function() {
		var family = $(this).attr('href') ? $(this).attr('href').substring(1) : $(this).attr('id');
		blurconsole.model.search.loadMoreResults(family);
		return false;
	};

	showOptions = function() {

	};

	reviewTables = function() {
		var tableFound = false, tableMap;

		if (stateMap.$currentTable) {
			tableMap = blurconsole.model.tables.getAllEnabledTables();
			$.each(tableMap, function(cluster, tables){
				var tableList = $.map(tables, function(t){ return t.name; });
				if (tableList.indexOf(stateMap.$currentTable) > -1) {
					tableFound = true;
				}
			});
		}

		if (tableFound) {
			jqueryMap.$tableWarning.hide();
			loadTableList();
		} else if (stateMap.$currentTable) {
			jqueryMap.$tableWarning.show();
		} else {
			loadTableList();
		}
	};

	drawResultHolders = function() {
		var familyMarkup = '', allFamilies, extraFamilies = [], parsedFamilies = blurconsole.utils.findFamilies(stateMap.$currentQuery), sortedFamilies;

		jqueryMap.$resultsHolder.html('');

		// Redraw families
		allFamilies = blurconsole.model.tables.getFamilies(stateMap.$currentTable);
		extraFamilies = blurconsole.utils.reject(allFamilies, function(fam){ return parsedFamilies.indexOf(fam) >= 0; });

		parsedFamilies.sort();
		extraFamilies.sort();

		sortedFamilies = parsedFamilies.concat(extraFamilies);

		$.each(sortedFamilies, function(i, fam) {
			var famId = blurconsole.browserUtils.cleanId(fam);
			familyMarkup += '<div class="panel panel-default"><div class="panel-heading">';
			familyMarkup += '<h4 class="panel-title"><a data-toggle="collapse" data-parent="#results" href="#' + famId + '">' + fam + '</a></h4></div>';
			familyMarkup += '<div id="' + famId + '" class="panel-collapse collapse' + (parsedFamilies.indexOf(fam) >= 0 ? ' in' : '') + '">';
			familyMarkup += '<div class="panel-body"><img src="img/ajax-loader.gif"></div></div></div>';
		});

		jqueryMap.$resultsHolder.html(familyMarkup);
		fixPanelWidths();
	};

	fixPanelWidths = function() {
		var allPanels = jqueryMap.$resultsHolder.find('.panel-collapse');
		if (allPanels.length > 0) {
			var width = $(allPanels[0]).parent().width() - 30;
			allPanels.width(width);
		}
	};

	drawResults = function(evt, families) {
		var results = blurconsole.model.search.getResults();
		jqueryMap.$countHolder.html('<small>Found ' + blurconsole.model.search.getTotal() + ' total results</small>');
		//jqueryMap.$facetTrigger.show();

		if (typeof families !== 'undefined' && families !== null) {
			$.each(families, function(i, fam) {
				var famResults = results[fam],
					famId = '#' + blurconsole.browserUtils.cleanId(fam),
					famHolder = $(famId + ' .panel-body'),
					table = '<table class="table table-condensed table-hover table-bordered"><thead><tr>',
					cols;

				if (famResults.length === 0) {
					famHolder.html('<div class="alert alert-info">No Data Found</div>');
				} else {
					if (blurconsole.utils.keys(famResults[0]).indexOf('rowid') === -1 ) {
						// Record results
						cols = getColList(famResults[0]);

						$.each(cols, function(i, col) {
							table += '<th>' + col + '</th>';
						});
						table += '</tr></thead><tbody>';
						$.each(famResults, function(i, row) {
							table += '<tr>';
							$.each(cols, function(c, col) {
								table += '<td>' + (row[col] || '') + '</td>';
							});
							table += '</tr>';
						});
					} else {
						// Row results
						$.each(famResults, function(i, row){
							if (row.records.length > 0) {
								var tmpCols = getColList(row.records[0]);
								if (tmpCols.length > 0) {
									cols = tmpCols;
									return false;
								}
							}
						});

						cols = cols || [];

						$.each(cols, function(i, col) {
							table += '<th>' + col + '</th>';
						});
						table += '</tr></thead><tbody>';
						$.each(famResults, function(r, row) {
							table += '<tr class="row-separator"><td colspan="' + (cols.length === 0 ? 1 : cols.length) + '">' + (r+1) + '. <strong>rowid:</strong> ' + row.rowid + ' (<em>' + (row.records === null ? 0 : row.records.length) + ' records</em>)</td></tr>';
							if (row.records === null || row.records.length === 0) {
								table += '<tr><td colspan="' + (cols.length === 0 ? 1 : cols.length) + '"><em>No Data Found</em></td></tr>';
							} else {
								$.each(row.records, function(i, rec) {
									table += '<tr>';
									$.each(cols, function(c, col) {
										table += '<td>' + (rec[col] || '') + '</td>';
									});
									table += '</tr>';
								});
							}
						});
					}

					table += '</tbody></table>';

					if (famResults.length < blurconsole.model.search.getTotal()) {
						table += '<div class="pull-left"><a href="' + famId + '" class="btn btn-primary nextPage">Load More...</a></div>';
					}

					famHolder.html(table);
				}
				if (!$(famId).hasClass('loaded')) {
					$(famId).addClass('loaded');
				}
			});
			fixPanelWidths();
		}
	};

	getColList = function(row) {
		var cols = blurconsole.utils.reject(blurconsole.utils.keys(row), function(i) {
			return i === 'recordid';
		});

		if (cols.length === 0) {
			return [];
		}

		cols.sort();

		cols = ['recordid'].concat(cols);
		return cols;
	};

	loadTableList = function() {
		var tableMap = blurconsole.model.tables.getAllEnabledTables();

		jqueryMap.$tableSelectorStatusOption.html('Loading Tables...');
		jqueryMap.$tableField.find('optgroup').remove();

		$.each(tableMap, function(cluster, tables) {
			var optGroupString;

			if (tables.length > 0) {
				optGroupString = '<optgroup label="' + cluster + '">';
				$.each(tables, function(t, table){
					optGroupString += '<option value="' + table.name + '"' + (table.name === stateMap.$currentTable ? ' selected' : '') + '>' + table.name + '</option>';
				});
				optGroupString += '</optgroup>';
				jqueryMap.$tableField.append(optGroupString);
			}
		});

		jqueryMap.$tableSelectorStatusOption.html('Choose Table');
	};

	popupFacetDialog = function() {
		jqueryMap.facetModal = $(blurconsole.browserUtils.modal('facetDialog', 'Facets for Current Search', 'TBD', null, 'large'));
		jqueryMap.facetModal.modal();
	};

	initModule = function($container) {
		$container.load(configMap.view, function() {
			stateMap.$container = $container;
			setJqueryMap();
			$.gevent.subscribe(jqueryMap.$container, 'tables-updated', reviewTables);
			$.gevent.subscribe(jqueryMap.$container, 'results-updated', drawResults);
			registerPageEvents();
			loadTableList();

			var startupMap = $.uriAnchor.makeAnchorMap();

			if (startupMap._tab) {
				stateMap.$currentQuery = startupMap._tab.query;
				jqueryMap.$queryField.val(stateMap.$currentQuery);
				stateMap.$currentTable = startupMap._tab.table;
				jqueryMap.$tableField.val(stateMap.$currentTable);
				stateMap.$rowRecordOption = startupMap._tab.rr;
			}

			updateOptionDisplay();
			stateMap.loaded = true;
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