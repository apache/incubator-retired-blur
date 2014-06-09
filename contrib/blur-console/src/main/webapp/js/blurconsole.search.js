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
    
    //----------------------------- Configuration and State --------------------------------
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
	jqueryMap = {};
    
    //----------------- Private Methods ----------------------------------
	function _setJqueryMap() {
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
	}

	function _registerPageEvents() {
		$('#searchTrigger').on('click', _sendSearch);
		$('#results').on('shown.bs.collapse', '.panel-collapse:not(.loaded)', _getMoreData);
		$('#results').on('click', '.nextPage', _getMoreData);
		$('#searchOptionsTrigger').popover({
			html: true,
			placement: 'bottom',
			title: 'Extra Search Options',
			container: 'body',
			content: configMap.optionsHtml
		});
		$('#searchOptionsTrigger').on('shown.bs.popover', _updateOptionPopover);
		$(document).on('change', '.popover select', _persistOptions);
		jqueryMap.$facetTrigger.on('click', _popupFacetDialog);
	}

	function _unregisterPageEvents() {
		$('#searchTrigger').off('click');
		$('#results').off('shown.bs.collapse');
		$('#results').off('click');
		$('#searchOptionsTrigger').popover('destroy');
		$('#searchOptionsTrigger').off('shown.bs.popover');
		$(document).off('change');
		//jqueryMap.$facetTrigger.off('click');
	}
    
    function _getColList(row) {
		var cols = blurconsole.utils.reject(blurconsole.utils.keys(row), function(i) {
			return i === 'recordid';
		});

		if (cols.length === 0) {
			return [];
		}

		cols.sort();

		cols = ['recordid'].concat(cols);
		return cols;
	}
    
    //------------------------------ Event Handlers and DOM Methods ---------------------
	function _updateOptionDisplay() {
		var displayText = '';
		displayText += configMap.superQueryMap[stateMap.$rowRecordOption];
		jqueryMap.$optionsDisplay.html(displayText);
	}

	function _updateOptionPopover() {
		if ($('#superQuery').length > 0) {
			$('#superQuery').val(stateMap.$rowRecordOption);
		}
	}

	function _persistOptions() {
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
			_sendSearch();
		}
		_updateOptionDisplay();
		$('#searchOptionsTrigger').popover('hide');
	}

	function _sendSearch() {
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
		_drawResultHolders();
		jqueryMap.$countHolder.html('');
		blurconsole.model.search.runSearch(stateMap.$currentQuery, stateMap.$currentTable, {start: 0, fetch: 10, rowRecordOption: stateMap.$rowRecordOption});
	}

	function _getMoreData(evt) {
		var family = $(evt.currentTarget).attr('href') ? $(evt.currentTarget).attr('href').substring(1) : $(evt.currentTarget).attr('id');
		blurconsole.model.search.loadMoreResults(family);
		return false;
	}

	function _reviewTables() {
		var tableFound = false;

		if (stateMap.$currentTable) {
			var tableMap = blurconsole.model.tables.getAllEnabledTables();
			$.each(tableMap, function(cluster, tables){
				var tableList = $.map(tables, function(t){ return t.name; });
				if (tableList.indexOf(stateMap.$currentTable) > -1) {
					tableFound = true;
				}
			});
		}

		if (tableFound) {
			jqueryMap.$tableWarning.hide();
			_loadTableList();
		} else if (stateMap.$currentTable) {
			jqueryMap.$tableWarning.show();
		} else {
			_loadTableList();
		}
	}

	function _drawResultHolders() {
		var familyMarkup = '', parsedFamilies = blurconsole.utils.findFamilies(stateMap.$currentQuery);

		jqueryMap.$resultsHolder.html('');

		// Redraw families
		var allFamilies = blurconsole.model.tables.getFamilies(stateMap.$currentTable);
		var extraFamilies = blurconsole.utils.reject(allFamilies, function(fam){ return parsedFamilies.indexOf(fam) >= 0; });

		parsedFamilies.sort();
		extraFamilies.sort();

		var sortedFamilies = parsedFamilies.concat(extraFamilies);

		$.each(sortedFamilies, function(i, fam) {
			var famId = blurconsole.browserUtils.cleanId(fam);
			familyMarkup += '<div class="panel panel-default"><div class="panel-heading">';
			familyMarkup += '<h4 class="panel-title"><a data-toggle="collapse" data-parent="#results" href="#' + famId + '">' + fam + '</a></h4></div>';
			familyMarkup += '<div id="' + famId + '" class="panel-collapse collapse' + (parsedFamilies.indexOf(fam) >= 0 ? ' in' : '') + '">';
			familyMarkup += '<div class="panel-body"><img src="img/ajax-loader.gif"></div></div></div>';
		});

		jqueryMap.$resultsHolder.html(familyMarkup);
		_fixPanelWidths();
	}

	function _fixPanelWidths() {
		var allPanels = jqueryMap.$resultsHolder.find('.panel-collapse');
		if (allPanels.length > 0) {
			var width = $(allPanels[0]).parent().width() - 30;
			allPanels.width(width);
		}
	}

	function _drawResults(evt, families) {
		var results = blurconsole.model.search.getResults();
		jqueryMap.$countHolder.html('<small>Found ' + blurconsole.model.search.getTotal() + ' total results</small>');
		//jqueryMap.$facetTrigger.show();

		if (typeof families !== 'undefined' && families !== null) {
			$.each(families, function(i, fam) {
				var famResults = results[fam],
					famId = '#' + blurconsole.browserUtils.cleanId(fam),
					famHolder = $(famId + ' .panel-body');

				if (typeof famResults === 'undefined' || famResults.length === 0) {
					famHolder.html('<div class="alert alert-info">No Data Found</div>');
				} else {
					var table;
					var cols;
					if (blurconsole.utils.keys(famResults[0]).indexOf('rowid') === -1 ) {
						// Record results
						table = '<table class="table table-condensed table-hover table-bordered"><thead><tr>';
						cols = _getColList(famResults[0]);

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
						table += '</tbody></table>';
					} else {
						// Row results
						$.each(famResults, function(i, row){
							if (row.records.length > 0) {
								var tmpCols = _getColList(row.records[0]);
								if (tmpCols.length > 0) {
									cols = tmpCols;
									return false;
								}
							}
						});

						cols = cols || [];
						table = '';

						$.each(famResults, function(r, row) {
							table += '<table class="table table-condensed table-hover table-bordered"><thead>';
							table += '<tr class="row-separator"><th colspan="' + (cols.length === 0 ? 1 : cols.length) + '">' + (r+1) + '. <strong>rowid:</strong> ' + row.rowid + ' (<em>' + (row.records === null ? 0 : row.records.length) + ' records</em>)</th></tr>';
							table += '<tr>';
							$.each(cols, function(i, col) {
								table += '<th>' + col + '</th>';
							});
							table += '</tr></thead><tbody>';

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
							table += '</tbody></table>';
						});
					}

					if (famResults.length < blurconsole.model.search.getTotal()) {
						table += '<div class="pull-left"><a href="' + famId + '" class="btn btn-primary nextPage">Load More...</a></div>';
					}

					famHolder.html(table);
				}
				if (!$(famId).hasClass('loaded')) {
					$(famId).addClass('loaded');
				}
			});
			_fixPanelWidths();
		}
	}

	function _loadTableList() {
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
	}

	function _popupFacetDialog() {
		jqueryMap.facetModal = $(blurconsole.browserUtils.modal('facetDialog', 'Facets for Current Search', 'TBD', null, 'large'));
		jqueryMap.facetModal.modal();
	}

    //--------------------------------- Public API ------------------------------------------
	function initModule($container) {
		$container.load(configMap.view, function() {
			stateMap.$container = $container;
			_setJqueryMap();
			$.gevent.subscribe(jqueryMap.$container, 'tables-updated', _reviewTables);
			$.gevent.subscribe(jqueryMap.$container, 'results-updated', _drawResults);
			_registerPageEvents();
			_loadTableList();

			var startupMap = $.uriAnchor.makeAnchorMap();

			if (startupMap._tab) {
				stateMap.$currentQuery = startupMap._tab.query;
				jqueryMap.$queryField.val(stateMap.$currentQuery);
				stateMap.$currentTable = startupMap._tab.table;
				jqueryMap.$tableField.val(stateMap.$currentTable);
				stateMap.$rowRecordOption = startupMap._tab.rr;
			}

			_updateOptionDisplay();
			stateMap.loaded = true;
		});
		return true;
	}

	function unloadModule() {
		$.gevent.unsubscribe(jqueryMap.$container, 'tables-updated');
		_unregisterPageEvents();
	}

	return {
		initModule : initModule,
		unloadModule : unloadModule
	};
}());