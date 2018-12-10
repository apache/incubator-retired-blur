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
blurconsole.fake = (function() {
  'use strict';
  var frozen = false;
  var tableList, schema, nodeList, queries;

  //----------------------- Private Methods ----------------------
  function _randomNumber(max, includeZero) {
    var random = Math.random()*max;

    if (!includeZero) {
      random++;
    }

    return Math.floor(random);
  }

  function _randomBoolean() {
    return _randomNumber(2) % 2 === 0;
  }

  function _randomString(maxLength) {
    maxLength = typeof maxLength !== 'undefined' ? maxLength : 30;
    var text = '';
    var possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ';

    for( var i=0; i < Math.floor(Math.random() * maxLength + 1); i++ ) {
      text += possible.charAt(Math.floor(Math.random() * possible.length));
    }

    return text;
  }

  function _buildSchema() {
    var schema = {}, familyCount = _randomNumber(20), types = ['string', 'long', 'int', 'date', 'stored', 'customType'];

    for(var f=0; f < familyCount; f++) {
      var c, fam = {}, columnCount = _randomNumber(30);
      for(c=0; c < columnCount; c++) {
        var col_name = 'col' + c;
        if(_randomNumber(10) === 1) {
          col_name += '.sub';
        }
        fam[col_name] = {
          'fieldLess' : _randomBoolean(),
          'type' : types[_randomNumber(6, true)],
          'extra' : null
        };

        if (_randomBoolean()) {
          var e, extraPropCount = _randomNumber(3), props = {};
          for (e=0; e < extraPropCount; e++) {
            props['extra'+e] = 'val'+e;
          }
          fam[col_name].extra = props;
        }
      }
      schema['fam'+f] = fam;
    }
    return schema;
  }

  function _sendCallback(callback, data) {
    setTimeout(function() {
      callback(data);
    }, _randomNumber(1000));
  }

  function _toggleFreeze() {
    var button = $('#fake_freeze');
    if(button.html() === 'Freeze'){
      frozen = true;
      button.html('Unfreeze');
      console.log('Fake data frozen');
    } else {
      frozen = false;
      button.html('Freeze');
      console.log('Fake data resumed');
    }
  }

  //-------------------------- Public API ---------------------------
  function getTableList(callback) {
    //console.log('getting fake table list');
    if(!frozen || !tableList) {
      var clusters = ['prodA', 'prodB'], data = [];

      for (var i = 0; i < 5; i++) {
        var cluster = clusters[_randomNumber(2, true)];
        var rows = _randomNumber(1000);
        var records = _randomNumber(10000)+1000;
        var enabled = _randomBoolean();

        data.push({cluster:cluster, name:'testtable'+i, enabled:enabled, rows:rows, records:records, families: blurconsole.utils.keys(_buildSchema())});

      }
      tableList = {'tables':data, 'clusters':clusters};
    }
    _sendCallback(callback, tableList);
  }

  function getNodeList(callback) {
    //console.log('getting fake node list');
    if(!frozen || !nodeList) {
      var controllers = {online:[], offline:[]},
        clusters = [{name:'prodA', online:[], offline:[]}, {name:'prodB', online:[], offline:[]}],
        zookeepers = {online: [], offline:[]};

      for(var i = 0; i < 3; i++) {
        var state = _randomBoolean();
        if (state) {
          controllers.online.push('controller' + i + '.localhost');
          clusters[0].online.push('prodA.shard' + i + '.localhost');
          clusters[1].online.push('prodB.shard' + i + '.localhost');
          zookeepers.online.push('zookeeper' + i + '.localhost');
        } else {
          controllers.offline.push('controller' + i + '.localhost');
          clusters[0].offline.push('prodA.shard' + i + '.localhost');
          clusters[1].offline.push('prodB.shard' + i + '.localhost');
          zookeepers.offline.push('zookeeper' + i + '.localhost');
        }
      }
      nodeList = {controllers: controllers, clusters: clusters, zookeepers: zookeepers};
    }
    _sendCallback(callback, nodeList);
  }

  function getQueryPerformance(callback) {
    //console.log('getting fake query performance');
    _sendCallback(callback, _randomNumber(1000, true));
  }

  function getQueries(callback) {
    //console.log('getting fake queries');
    if(!frozen || !queries) {
      var randomQueries = [];

      for (var i=0; i < _randomNumber(50); i++) {
        randomQueries.push({
          uuid: _randomString(),
          user: 'user_' + _randomNumber(10, true),
          query: _randomString(500),
          table: 'testtable' + _randomNumber(5, true),
          state: _randomNumber(3, true),
          percent: _randomNumber(100, true),
          startTime: new Date().getTime()
        });
      }
      queries = { slowQueries : _randomNumber(10) === 1, queries : randomQueries };
    }
    _sendCallback(callback, queries);
  }

  function cancelQuery(table, uuid) {
    console.log('Fake sending request to cancel query [' + uuid + '] on table [' + table + ']');
  }

  function disableTable(table) {
    console.log('Fake sending request to disable table [' + table + ']');
  }

  function enableTable(table) {
    console.log('Fake sending request to enable table [' + table + ']');
  }

  function deleteTable(table, includeFiles) {
    console.log('Fake sending request to delete table [' + table + '] with files [' + includeFiles + ']');
  }

  function getSchema(table, callback) {
    console.log('getting fake schema for table [' + table + ']');
    if(!frozen || !schema){
      schema = _buildSchema();
    }
    _sendCallback(callback, schema);
  }

  function findTerms(table, family, column, startsWith, callback) {
    console.log('getting fake terms from [' + table + '] for family [' + family + '] and column [' + column + '] starting with [' + startsWith + ']');
    var terms = [];

    for (var i = 0; i < 10; i++) {
      var randStr = _randomString();
      if (startsWith) {
        randStr = startsWith + randStr;
      }
      terms.push(randStr);
    }

    terms = terms.sort(function (a, b) {
      return a.toLowerCase().localeCompare(b.toLowerCase());
    });
    _sendCallback(callback, terms);
  }

  function copyTable(srcTable, destTable, destLocation, cluster) {
    console.log('Fake sending request to copy table [' + srcTable + '] to [' + destTable + '] on cluster [' + cluster + '] stored in location [' + destLocation + ']');
  }

  function sendSearch(query, table, args, callback) {
    console.log('sending fake search [' + query + '] on table [' + table + ']');

    var fams = args.families, results = {}, total = (fams !== null && fams.indexOf('rowid') >= 0) ? 1 : _randomNumber(5000);

    if (fams === null || fams.length === 0) {
      $.each(tableList.tables, function(i, t) {
        if (t.name === table) {
          var tFams = t.families;
          tFams.sort();
          fams = [tFams[0]];
          return false;
        }
      });
    }

    if (fams !== null) {
      $.each(fams, function(i, fam){
        var cols = _randomNumber(30, true), toFetch = (fams !== null && fams.indexOf('rowid') >= 0)? 1 : args.fetch;
        if (total - args.start < toFetch) {
          toFetch = total - args.start;
        }

        if (args.rowRecordOption === 'recordrecord') {
          results[fam] = [];
          for (var recordIndex = 0; recordIndex < _randomNumber(toFetch); recordIndex++) {
            var recordRow = {};
            recordRow.recordid = _randomNumber(1000000).toString();
            for (var recordColIndex=0; recordColIndex < cols; recordColIndex++) {
              recordRow['col'+recordColIndex] = _randomString();
            }
            results[fam].push(recordRow);
          }
        } else {
          results[fam] = [];
          for (var rowIndex = 0; rowIndex < _randomNumber(toFetch); rowIndex++) {
            var rowid = _randomNumber(10000000).toString();
            results[fam][rowIndex] = {rowid: rowid, records: []};
            for (var rowRecordIndex = 0; rowRecordIndex < _randomNumber(10); rowRecordIndex++) {
              var row = {};
              row.recordid = _randomNumber(1000000).toString();
              for (var rowRecordColIndex=0; rowRecordColIndex < cols; rowRecordColIndex++) {
                row['col'+rowRecordColIndex] = _randomString();
              }
              results[fam][rowIndex].records.push(row);
            }
          }
        }
      });
    }

    if (fams.indexOf('rowid') >= 0) {
      _sendCallback(callback, { total: total, results: results, families: fams });
    } else {
      _sendCallback(callback, { families: fams, results: results, total: total });
    }
  }

  function runFacetCount(query, table, family, column, terms, callback) {
    console.log('sending fake facet count [' + terms + '] on table [' + table + '] on against query [' + query + ']');
    var data = {};
    $.each(terms, function(i, term){
      data[term] = _randomNumber(100, true);
    });
    _sendCallback(callback, data);
  }

  function initModule() {
    $('nav.navbar .pull-right').append('<button type="button" id="fake_freeze" class="btn btn-default btn-sm">Freeze</button>');
    $('#fake_freeze').click(_toggleFreeze);
  }

  return {
    initModule: initModule,
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
    copyTable : copyTable,
    sendSearch : sendSearch,
    runFacetCount: runFacetCount
  };
}());