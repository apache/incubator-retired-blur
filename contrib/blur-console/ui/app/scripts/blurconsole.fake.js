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
	var getTableList, getNodeList, getQueryPerformance, getQueries;

	getTableList = function() {
		var clusters = ['prodA', 'prodB'], data = [], i, cluster, rows, records, enabled;

		for (i = 0; i < 5; i++) {
			cluster = clusters[Math.floor(Math.random()*2)];
			rows = Math.floor(Math.random()*1000+1);
			records = Math.floor(Math.random()*10000+1001);
			enabled = Math.floor(Math.random()*2) === 1;

			data.push({cluster:cluster, name:'testtable'+i, enabled:enabled, rows:rows, records:records});

		}
		return data;
	};

	getNodeList = function() {
		var controllers = {online:[], offline:[]},
			clusters = [{name:'prodA', online:[], offline:[]}, {name:'prodB', online:[], offline:[]}],
			zookeepers = {online: [], offline:[]},
			i, state;

		for(i = 0; i < 3; i++) {
			state = Math.floor(Math.random()*2);
			if (state === 0) {
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
		return {controllers: controllers, clusters: clusters, zookeepers: zookeepers};
	};

	getQueryPerformance = function() {
		return Math.floor((Math.random()*1000) + 1);
	};

	getQueries = function() {
		return {
			slowQueries : Math.floor((Math.random()*100) + 1) % 10 === 0
		};
	};

	return {
		getTableList : getTableList,
		getNodeList : getNodeList,
		getQueryPerformance : getQueryPerformance,
		getQueries : getQueries
	};
}());