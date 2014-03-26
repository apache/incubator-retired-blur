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
/*jshint laxbreak: true */
/*global blurconsole:false */
blurconsole.schema = (function () {
	'use strict';
	var
		configMap = {
			mainHtml: String()
				+ '<div class="container-fluid">'
					+ '<div class="row">'
						+ '<div class="col-md-6">'
							+ '<div class="panel-group schemaList">'
							+ '</div>'
						+ '</div>'
						+ '<div class="col-md-6">'
							+ '<div class="row">'
								+ '<div class="col-md-12 schemaColumnInfo">'
									+ '<div class="schemaColumnDef in">'
										+ '<strong>Choose a column on the left to see def</strong>'
									+ '</div>'
								+ '</div>'
							+ '</div>'
							+ '<div class="row">'
								+ '<div class="col-md-12 schemaColumnTerms">'
									+ '<div class="input-group">'
										+ '<input class="form-control termSearch" placeholder="Starts With" type="text">'
										+ '<span class="input-group-btn">'
											+ '<button class="btn btn-default" type="button">Go!</button>'
										+ '</span>'
									+ '</div>'
									+ '<ul class="list-group termList"></ul>'
								+ '</div>'
							+ '</div>'
						+ '</div>'
					+ '</div>'
				+ '</div>'
		},
		stateMap = {},
		jqueryMap = {},
		initModule, showSchema, buildTreeSection, buildInfoSection, viewTerms, findTerms, loadTerms, switchToSearch;

	showSchema = function(event, table) {
		stateMap.table = table;
		stateMap.modalId = stateMap.table + '_modal';
		stateMap.schema = blurconsole.model.tables.getSchema(stateMap.table);

		jqueryMap.contentHolder = $(configMap.mainHtml);
		jqueryMap.contentHolder.find('.schemaList').html(buildTreeSection());
		jqueryMap.contentHolder.find('.schemaColumnInfo').append(buildInfoSection());

		jqueryMap.modal = $(blurconsole.browserUtils.modal(stateMap.modalId, 'Schema Definition for ' + stateMap.table, jqueryMap.contentHolder, null, 'large'));
		jqueryMap.modal.modal()
		.on('shown.bs.modal', function(e){
			jqueryMap.columnTermsSection = $('.schemaColumnTerms', jqueryMap.modal);
			jqueryMap.termSearch = $('.termSearch', jqueryMap.modal);
			jqueryMap.termList = $('.termList', jqueryMap.modal);
			jqueryMap.termSearchButton = $('.schemaColumnTerms button', jqueryMap.modal);
			$('.collapse', e.currentTarget).collapse({ toggle: false });
		})
		.on('hidden.bs.modal', function(e) {
			$(e.currentTarget).remove();
			jqueryMap.contentHolder.remove();
			jqueryMap = {};
			stateMap = {};
		})
		.on('click', 'li.schemaColumn', function() {
			var defId = $(this).find('a').attr('href');
			$('div.schemaColumnDef').removeClass('in');
			$(defId).addClass('in');
			jqueryMap.columnTermsSection.hide();
			return false;
		})
		.on('click', 'a.termsTrigger', viewTerms)
		.on('click', '.schemaColumnTerms button', findTerms)
		.on('click', '.searchTrigger', switchToSearch);
	};

	buildTreeSection = function() {
		var tree = '';
		$.each(stateMap.schema, function(family, cols){
			var famId = blurconsole.browserUtils.cleanId(family);
			tree += '<div class="panel panel-default"><div class="panel-heading">';
			tree += '<h4 class="panel-title"><a data-toggle="collapse" data-parent=".schemaList" href="#' + famId + '">' + family + '</a></h4></div>';
			tree += '<div id="' + famId + '" class="panel-collapse collapse"><div class="panel-body"><ul class="list-group">';
			$.each(cols, function(col, def) {
				var colId = blurconsole.browserUtils.cleanId(col);
				tree += '<li class="list-group-item schemaColumn"><a href="#' + famId + '_' + colId + '">' + col + '';
				if (def.type !== 'stored') {
					tree += ' <span class="badge">searchable</span>';
				}
				tree += '</a><div class="pull-right"><i class="glyphicon glyphicon-chevron-right"></i></div></li>';
			});
			tree += '</ul></div></div></div>';
		});
		return tree;
	};

	buildInfoSection = function() {
		var info = '';
		$.each(stateMap.schema, function(family, cols){
			var famId = blurconsole.browserUtils.cleanId(family);
			$.each(cols, function(col, def){
				var colId = blurconsole.browserUtils.cleanId(col);
				info += '<div class="schemaColumnDef" id="' + famId + '_' + colId + '"><ul class="list-group">';
				info += '<li class="list-group-item"><strong>Fieldless Searching:</strong> ' + blurconsole.browserUtils.booleanImg(def.fieldLess) + '</li>';
				info += '<li class="list-group-item"><strong>Field Type:</strong> ' + def.type + '</li>';
				if (def.extra) {
					$.each(def.extra, function(key, value) {
						info += '<li class="list-group-item"><strong>' + key + ':</strong> ' + value + '</li>';
					});
				}
				if (def.type !== 'stored') {
					info += '<li class="list-group-item"><a href="#" class="termsTrigger" data-fam="' + family + '" data-col="' + col + '">View Terms</a></li>';
				}
				info += '</ul></div>';
			});
		});
		return info;
	};

	viewTerms = function() {
		jqueryMap.termList.html('<div class="center-block"><img src="images/ajax-loader.gif"></div>');
		jqueryMap.termSearch.val('');
		jqueryMap.columnTermsSection.show();
		var $this = $(this);

		stateMap.termFamily = $this.data('fam');
		stateMap.termColumn = $this.data('col');

		jqueryMap.termSearchButton.trigger('click');
	};

	findTerms = function() {
		blurconsole.model.tables.findTerms(stateMap.table, stateMap.termFamily, stateMap.termColumn, jqueryMap.termSearch.val());
	};

	loadTerms = function() {
		var terms = Array.prototype.slice.call(arguments, 1);
		jqueryMap.termList.html('');
		$.each(terms, function(i, term){
			jqueryMap.termList.append('<li class="list-group-item">' + term + ' <span class="badge badge-success searchTrigger" title="Search for this value" data-value="' + term + '"><i class="glyphicon glyphicon-search"></i></span></li>');
		});
	};

	switchToSearch = function(){
		blurconsole.shell.changeAnchorPart({
			tab: 'search',
			_tab: {
				query: stateMap.termFamily + '.' + stateMap.termColumn + ':' + $(this).data('value')
			}
		});
	};

	initModule = function() {
		$.gevent.subscribe($(document), 'schema-show', showSchema);
		$.gevent.subscribe($(document), 'terms-updated', loadTerms);
	};

	return {
		initModule : initModule
	};
}());