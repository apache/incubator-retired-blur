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
blurconsole.data = (function() {
	'use strict';
	var getTableList, getNodeList, getQueryPerformance, getQueries, cancelQuery, disableTable, enableTable, deleteTable, getSchema, findTerms, sendSearch,
		logError;

	getTableList = function(callback) {
		$.getJSON('/service/tables', callback).fail(function(xhr) {
			logError(xhr.responseText, xhr.status, 'tables', callback);
		});
	};

	getNodeList = function(callback) {
		$.getJSON('/service/nodes', callback).fail(function(xhr) {
			logError(xhr.responseText, xhr.status, 'tables', callback);
		});
	};

	getQueryPerformance = function(callback) {
		$.getJSON('/service/queries/performance', callback).fail(function(xhr) {
			logError(xhr.responseText, xhr.status, 'tables', callback);
		});
	};

	getQueries = function(callback) {
		$.getJSON('/service/queries', callback).fail(function(xhr) {
			logError(xhr.responseText, xhr.status, 'tables', callback);
		});
	};

	cancelQuery = function(table, uuid) {
		$.ajax('/service/queries/' + uuid + '/cancel', {
			data: {
				table: table
			},
			error: function(xhr) {
				logError(xhr.responseText, xhr.status, 'tables');
			}
		});
	};

	disableTable = function(table) {
		$.ajax('/service/tables/' + table + '/disable', {
			error: function(xhr) {
				logError(xhr.responseText, xhr.status, 'tables');
			}
		});
	};

	enableTable = function(table) {
		$.ajax('/service/tables/' + table + '/enable', {
			error: function(xhr) {
				logError(xhr.responseText, xhr.status, 'tables');
			}
		});
	};

	deleteTable = function(table, includeFiles) {
		$.ajax('/service/tables/' + table + '/delete', {
			data: {
				includeFiles: includeFiles
			},
			error: function(xhr) {
				logError(xhr.responseText, xhr.status, 'tables');
			}
		});
	};

	getSchema = function(table, callback) {
		$.getJSON('/service/tables/' + table + '/schema', callback).fail(function(xhr) {
			logError(xhr.responseText, xhr.status, 'tables');
		});
	};

	findTerms = function(table, family, column, startsWith, callback) {
		$.getJSON('/service/tables/' + table + '/' + family + '/' + column + '/terms', {startsWith: startsWith}, callback).fail(function(xhr) {
			logError(xhr.responseText, xhr.status, 'tables');
		});
	};

	sendSearch = function(query, table, args, callback) {
		var params = $.extend({table:table, query:query}, args);
		$.ajax('/service/search', {
			'type': 'POST',
			'data': params,
			'success': callback,
			'error': function(xhr) {
				logError(xhr.responseText, xhr.status, 'tables');
			}
		});
	};

	logError = function(errorMsg, status, module, callback) {
		blurconsole.model.logs.logError(status + ' - ' + errorMsg, module);
		if (callback) {
			callback('error');
		}
	};

	return {
		getTableList : getTableList,
		getNodeList : getNodeList,
		getQueryPerformance : getQueryPerformance,
		getQueries : getQueries,
		cancelQuery : cancelQuery,
		disableTable : disableTable,
		enableTable : enableTable,
		deleteTable : deleteTable,
		getSchema : getSchema,
		findTerms : findTerms,
		sendSearch : sendSearch
	};
}());