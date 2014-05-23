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
	var getTableList, getNodeList, getQueryPerformance, getQueries, cancelQuery, disableTable, enableTable, deleteTable, getSchema, findTerms, sendSearch,
		buildSchema, randomNumber, randomBoolean, randomString;

	getTableList = function(callback) {
		console.log('getting fake table list');
		var clusters = ['prodA', 'prodB'], data = [], i, cluster, rows, records, enabled;

		for (i = 0; i < 5; i++) {
			cluster = clusters[randomNumber(2, true)];
			rows = randomNumber(1000);
			records = randomNumber(10000)+1000;
			enabled = randomBoolean();

			data.push({cluster:cluster, name:'testtable'+i, enabled:enabled, rows:rows, records:records, families: blurconsole.utils.keys(buildSchema())});

		}

		setTimeout(function() {
			callback(data);
		}, randomNumber(1000));
	};

	getNodeList = function(callback) {
		console.log('getting fake node list');
		var controllers = {online:[], offline:[]},
			clusters = [{name:'prodA', online:[], offline:[]}, {name:'prodB', online:[], offline:[]}],
			zookeepers = {online: [], offline:[]},
			i, state;

		for(i = 0; i < 3; i++) {
			state = randomBoolean();
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

		setTimeout(function(){
			callback({controllers: controllers, clusters: clusters, zookeepers: zookeepers});
		}, randomNumber(1000));
	};

	getQueryPerformance = function(callback) {
		console.log('getting fake query performance');

		setTimeout(function(){
			callback(randomNumber(1000));
		}, randomNumber(1000));
	};

	getQueries = function(callback) {
		console.log('getting fake queries');
		var queries = [];

		for (var i=0; i < randomNumber(50); i++) {
			queries.push({
				uuid: randomString(),
				user: 'user_' + randomNumber(10, true),
				query: randomString(),
				table: 'testtable' + randomNumber(5, true),
				state: randomNumber(3, true),
				percent: randomNumber(100, true),
				startTime: new Date().getTime()
			});
		}

		setTimeout(function(){
			callback({
				slowQueries : randomNumber(100) % 10 === 0,
				queries : queries
			});
		}, randomNumber(1000));
	};

	cancelQuery = function(table, uuid) {
		console.log('Fake sending request to cancel query [' + uuid + '] on table [' + table + ']');
	};

	disableTable = function(table) {
		console.log('Fake sending request to disable table [' + table + ']');
	};

	enableTable = function(table) {
		console.log('Fake sending request to enable table [' + table + ']');
	};

	deleteTable = function(table, includeFiles) {
		console.log('Fake sending request to delete table [' + table + '] with files [' + includeFiles + ']');
	};

	getSchema = function(table, callback) {
		console.log('getting fake schema for table [' + table + ']');
		setTimeout(function() {
			callback(buildSchema());
		}, randomNumber(1000));
	};

	buildSchema = function() {
		var f, schema = {}, familyCount = randomNumber(20), types = ['string', 'long', 'int', 'date', 'stored', 'customType'];

		for(f=0; f < familyCount; f++) {
			var c, fam = {}, columnCount = randomNumber(30);
			for(c=0; c < columnCount; c++) {
				fam['col'+c] = {
					'fieldLess' : randomBoolean(),
					'type' : types[randomNumber(6, true)],
					'extra' : null
				};

				if (randomBoolean()) {
					var e, extraPropCount = randomNumber(3), props = {};
					for (e=0; e < extraPropCount; e++) {
						props['extra'+e] = 'val'+e;
					}
					fam['col'+c].extra = props;
				}
			}
			schema['fam'+f] = fam;
		}
		return schema;
	};

	findTerms = function(table, family, column, startsWith, callback) {
		console.log('getting fake terms from [' + table + '] for family [' + family + '] and column [' + column + '] starting with [' + startsWith + ']');

		var terms = [];

		for (var i = 0; i < 10; i++) {
			var randStr = randomString();
			if (startsWith) {
				randStr = startsWith + randStr;
			}
			terms.push(randStr);
		}

		terms = terms.sort(function (a, b) {
			return a.toLowerCase().localeCompare(b.toLowerCase());
		});

		callback(terms);
	};

	sendSearch = function(query, table, args, callback) {
		console.log('sending fake search [' + query + '] on table [' + table + ']');

		var fams = args.families, results = {}, total = randomNumber(1000);

		$.each(fams, function(i, fam){
			var cols = randomNumber(30, true), toFetch = args.fetch;
			if (total - args.start < toFetch) {
				toFetch = total - args.start;
			}
			results[fam] = [];
			for (var r = 0; r < randomNumber(toFetch); r++) {
				var row = {};
				for (var c=0; c < cols; c++) {
					row['col'+c] = randomString();
				}
				results[fam].push(row);
			}
		});

		callback({
			families: fams,
			results: results,
			total: total
		});
	};

	randomNumber = function(max, includeZero) {
		var random = Math.random()*max;

		if (!includeZero) {
			random++;
		}

		return Math.floor(random);
	};

	randomBoolean = function() {
		return randomNumber(2) % 2 === 0;
	};

	randomString = function() {
		var text = '';
		var possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

		for( var i=0; i < Math.floor(Math.random() * 30 + 1); i++ ) {
			text += possible.charAt(Math.floor(Math.random() * possible.length));
		}

		return text;
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