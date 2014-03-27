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
blurconsole.queries = (function () {
	'use strict';
	var configMap = {
		view : 'views/queries.tpl.html'
	},
	stateMap = { $container : null },
	jqueryMap = {},
	setJqueryMap, initModule, unloadModule, updateQueryList, waitForData, registerPageEvents, unregisterPageEvents;

	setJqueryMap = function() {
		var $container = stateMap.$container;
		jqueryMap = {
			$container : $container,
			$queryInfoHolder : $('#queryInfoHolder')
		};
	};

	initModule = function($container) {
		$container.load(configMap.view, function() {
			stateMap.$container = $container;
			setJqueryMap();
			$.gevent.subscribe(jqueryMap.$container, 'queries-updated', updateQueryList);
			//waitForData();
			//registerPageEvents();
		});
		return true;
	};

	unloadModule = function() {
		$.gevent.unsubscribe(jqueryMap.$container, 'queries-updated');
		//unregisterPageEvents();
	};

	return {
		initModule : initModule,
		unloadModule : unloadModule
	};
}());