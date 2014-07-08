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

  //------------------- Private methods --------------------------
  function _logError (errorMsg, status, module, callback) {
    blurconsole.model.logs.logError(status + ' - ' + errorMsg, module);
    if (callback) {
      callback('error');
    }
  }

  //------------------- Public API -------------------------------
  function getTableList(callback) {
    $.getJSON('/service/tables', callback).fail(function(xhr) {
      _logError(xhr.responseText, xhr.status, 'tables', callback);
    });
  }

  function getNodeList(callback) {
    $.getJSON('/service/nodes', callback).fail(function(xhr) {
      _logError(xhr.responseText, xhr.status, 'tables', callback);
    });
  }
  function getQueryPerformance(callback) {
    $.getJSON('/service/queries/performance', callback).fail(function(xhr) {
      _logError(xhr.responseText, xhr.status, 'tables', callback);
    });
  }

  function getQueries(callback) {
    $.getJSON('/service/queries', callback).fail(function(xhr) {
      _logError(xhr.responseText, xhr.status, 'tables', callback);
    });
  }

  function cancelQuery(table, uuid) {
    $.ajax('/service/queries/' + uuid + '/cancel', {
      data: {
        table: table
      },
      error: function(xhr) {
        _logError(xhr.responseText, xhr.status, 'tables');
      }
    });
  }

  function disableTable(table) {
    $.ajax('/service/tables/' + table + '/disable', {
      error: function(xhr) {
        _logError(xhr.responseText, xhr.status, 'tables');
      }
    });
  }

  function enableTable (table){
    $.ajax('/service/tables/' + table + '/enable', {
      error: function(xhr) {
        _logError(xhr.responseText, xhr.status, 'tables');
      }
    });
  }

  function deleteTable (table, includeFiles) {
    $.ajax('/service/tables/' + table + '/delete', {
      data: {
        includeFiles: includeFiles
      },
      error: function(xhr) {
        _logError(xhr.responseText, xhr.status, 'tables');
      }
    });
  }

  function getSchema(table, callback) {
    $.getJSON('/service/tables/' + table + '/schema', callback).fail(function(xhr) {
      _logError(xhr.responseText, xhr.status, 'tables');
    });
  }

  function findTerms (table, family, column, startsWith, callback) {
    $.getJSON('/service/tables/' + table + '/' + family + '/' + column + '/terms', {startsWith: startsWith}, callback).fail(function(xhr) {
      _logError(xhr.responseText, xhr.status, 'tables');
    });
  }

  function sendSearch(query, table, args, callback) {
    var params = $.extend({table:table, query:query}, args);
    $.ajax('/service/search', {
      'type': 'POST',
      'data': params,
      'success': callback,
      'error': function(xhr) {
        _logError(xhr.responseText, xhr.status, 'tables');
      }
    });
  }

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