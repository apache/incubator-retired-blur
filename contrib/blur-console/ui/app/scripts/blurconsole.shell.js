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

/**
 * blurconsole.shell.js
 * Shell module for Blur Console
 */
/* global blurconsole:false, $:false */
blurconsole.shell = (function () {
	'use strict';
	var configMap = {
		anchorSchemaMap : {
			tab : { dashboard : true, tables : true, queries : true, search : true },
			_tab : { query: true, table: true, rr: true }
		},
		defaultTab : 'dashboard',
		allTabs : ['dashboard', 'tables', 'queries', 'search']
	},
	stateMap = {
		$container : null,
		currentTab : null,
		anchorMap  : {}
	},
	jqueryMap = {},
	copyAnchorMap, setJqueryMap, switchView,
	changeAnchorPart, onHashChange,
	onClickTab, initModule;

	copyAnchorMap = function () {
		return $.extend( true, {}, stateMap.anchorMap );
	};

	setJqueryMap = function () {
		var $container = stateMap.$container;
		jqueryMap = {
			$container   : $container,
			$sideNavTabs : $('.side-nav a')
		};
	};

	switchView = function ( tab ) {
		var i;

		if (stateMap.currentTab !== tab) {
			for ( i = 0; i < configMap.allTabs.length; i++ ) {
				if (blurconsole[configMap.allTabs[i]]) {
					blurconsole[configMap.allTabs[i]].unloadModule();
				}
			}

			stateMap.currentTab = tab;
			jqueryMap.$sideNavTabs.removeClass('active');
			jqueryMap.$sideNavTabs.filter('a[href$="' + tab + '"]').addClass('active');
			if (blurconsole[tab]) {
				blurconsole[tab].initModule( jqueryMap.$container );
			}
		}

		return true;
	};

	changeAnchorPart = function ( argMap ) {
		var anchorMapRevise = copyAnchorMap(),
			boolReturn = true,
			keyName, keyNameDep;

		KEYVAL:
		for ( keyName in argMap ) {
			if ( argMap.hasOwnProperty( keyName ) ) {
				if ( keyName.indexOf( '_' ) === 0 ) { continue KEYVAL; }
				anchorMapRevise[keyName] = argMap[keyName];
				keyNameDep = '_' + keyName;
				if ( argMap[keyNameDep] ) {
					anchorMapRevise[keyNameDep] = argMap[keyNameDep];
				} else {
					delete anchorMapRevise[keyNameDep];
					delete anchorMapRevise['_s' + keyNameDep];
				}
			}
		}

		try {
			$.uriAnchor.setAnchor( anchorMapRevise );
		} catch ( error ) {
			$.uriAnchor.setAnchor( stateMap.anchorMap, null, true );
			boolReturn = false;
		}

		return boolReturn;
	};

	onHashChange = function () {
		var anchorMapPrevious = copyAnchorMap(),
			anchorMapProposed,
			_sTabPrevious, _sTabProposed,
			sTabProposed;

		try { anchorMapProposed = $.uriAnchor.makeAnchorMap(); }
		catch ( error ) {
			$.uriAnchor.setAnchor( anchorMapPrevious, null, true );
			return false;
		}

		stateMap.anchorMap = anchorMapProposed;

		_sTabPrevious = anchorMapPrevious._s_tab; // jshint ignore:line
		_sTabProposed = anchorMapProposed._s_tab; // jshint ignore:line

		if ( ! anchorMapPrevious || _sTabPrevious !== _sTabProposed ){
			sTabProposed = anchorMapProposed.tab;
			switch ( sTabProposed ) {
				case 'dashboard':
				case 'tables':
				case 'queries':
				case 'search':
					switchView( sTabProposed );
					break;
				default:
					$.uriAnchor.setAnchor( anchorMapPrevious, null, true );
			}
		}

		return false;
	};

	onClickTab = function ( ) {
		var target = $(this);
		changeAnchorPart({
			tab : target.attr('href').substring(2)
		});
		return false;
	};

	initModule = function( $container ) {
		stateMap.$container = $container;
		setJqueryMap();

		blurconsole.schema.initModule();

		$('.side-nav li').tooltip();

		jqueryMap.$sideNavTabs.click( onClickTab );

		$.uriAnchor.configModule({
			schema_map : configMap.anchorSchemaMap // jshint ignore:line
		});

		$(window).bind('hashchange', onHashChange).trigger('hashchange');

		var startupMap = $.uriAnchor.makeAnchorMap();

		if ( !startupMap.tab ) {
			changeAnchorPart({
				tab: configMap.defaultTab
			});
		}
	};

	return {
		initModule: initModule,
		changeAnchorPart : changeAnchorPart
	};
}());