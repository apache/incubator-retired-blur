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
blurconsole.fake = (function() {
	'use strict';
	var getTableList, getNodeList, getQueryPerformance;

	getTableList = function() {
		return [
			{cluster: 'prodA', name: 'testtable1', enabled: true, rows: 1000, records: 10000},
			{cluster: 'prodA', name: 'testtable2', enabled: true, rows: 1000, records: 10000},
			{cluster: 'prodA', name: 'testtable3', enabled: false, rows: 1000, records: 10000},
			{cluster: 'prodB', name: 'testtable4', enabled: true, rows: 1000, records: 10000},
			{cluster: 'prodB', name: 'testtable5', enabled: false, rows: 1000, records: 10000},
		];
	};

	getNodeList = function() {
		return {
			controllers: {
				online: ['controller1.localhost', 'controller2.localhost'],
				offline: ['controller3.localhost']
			},
			clusters: [
				{name: 'prodA', online: [], offline: ['shard1.localhost']},
				{name: 'prodB', online: ['shard2.localhost'], offline: ['shard4.localhost']}
			],
			zookeepers: {
				online: ['zookeeper1.localhost'],
				offline: []
			}
		};
	};

	getQueryPerformance = function() {
		return Math.floor((Math.random()*1000) + 1);
	};

	return {
		getTableList : getTableList,
		getNodeList : getNodeList,
		getQueryPerformance : getQueryPerformance
	};
}());