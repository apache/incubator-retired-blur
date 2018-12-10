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
    
    //---------------------------- Configuration and State ----------------------------
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
  jqueryMap = {};
    
    //---------------------------- Private Methods -------------------------
  function _setJqueryMap() {
    var $container = stateMap.$container;
    jqueryMap = {
      $container   : $container,
      $sideNavTabs : $('.side-nav a')
    };
  }
    
  function _copyAnchorMap() {
    return $.extend( true, {}, stateMap.anchorMap );
  }
    
  function _switchView( tab ) {
    if (stateMap.currentTab !== tab) {
      for ( var i = 0; i < configMap.allTabs.length; i++ ) {
        if (blurconsole[configMap.allTabs[i]]) {
          blurconsole[configMap.allTabs[i]].unloadModule();
        }
      }

      stateMap.currentTab = tab;
      jqueryMap.$sideNavTabs.removeClass('active');
      jqueryMap.$sideNavTabs.filter('a[href$="' + tab + '"]').addClass('active');
      if (blurconsole[tab]) {
        blurconsole[tab].initModule( jqueryMap.$container );
      } else {
        changeAnchorPart({
          tab : 'dashboard'
        });
      }
    } else if (blurconsole[tab].anchorChanged) {
      blurconsole[tab].anchorChanged();
    }

    return true;
  }
    
    //---------------------------- Event Handlers and DOM Methods ----------
  function _onHashChange() {
    var anchorMapPrevious = _copyAnchorMap(), anchorMapProposed;

    try {
      anchorMapProposed = $.uriAnchor.makeAnchorMap();
    } catch ( error ) {
      $.uriAnchor.setAnchor( anchorMapPrevious, null, true );
      return false;
    }

    stateMap.anchorMap = anchorMapProposed;

    var _sTabPrevious = anchorMapPrevious._s_tab; // jshint ignore:line
    var _sTabProposed = anchorMapProposed._s_tab; // jshint ignore:line

    if ( ! anchorMapPrevious || _sTabPrevious !== _sTabProposed ){
      var sTabProposed = anchorMapProposed.tab;
      switch ( sTabProposed ) {
        case 'dashboard':
        case 'tables':
        case 'queries':
        case 'search':
          _switchView( sTabProposed );
          break;
        default:
          $.uriAnchor.setAnchor( anchorMapPrevious, null, true );
      }
    }

    return false;
  }
    
  function _onClickTab(evt) {
    var target = $(evt.currentTarget);
    changeAnchorPart({
      tab : target.attr('href').split('=')[1]
    });
    return false;
  }
    
    //---------------------------- Public API ------------------------------
  function changeAnchorPart( argMap ) {
    var anchorMapRevise = _copyAnchorMap(), boolReturn = true;

    KEYVAL:
    for ( var keyName in argMap ) {
      if ( argMap.hasOwnProperty( keyName ) ) {
        if ( keyName.indexOf( '_' ) === 0 ) { continue KEYVAL; }
        anchorMapRevise[keyName] = argMap[keyName];
        var keyNameDep = '_' + keyName;
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
  }

  function initModule( $container ) {
    stateMap.$container = $container;
    _setJqueryMap();

    blurconsole.schema.initModule();
    blurconsole.facets.initModule();
    blurconsole.logging.initModule();

    $('#dashboard_tab').show();
    $('#tables_tab').show();
    $('#queries_tab').show();

    if(blurconsole.auth.hasRole('searcher')) {
      $('#search_tab').show();
    } else {
      configMap.allTabs.splice(configMap.allTabs.indexOf('search'), 1);
      configMap.anchorSchemaMap.tab.search = false;
      blurconsole.search = null;
      $('#search_tab').remove();
    }

    $('#view_logging_trigger').on('click', function() {
      $.gevent.publish('show-logging');
    });

    $('.side-nav li').tooltip();

    jqueryMap.$sideNavTabs.click( _onClickTab );

    $.uriAnchor.configModule({
      schema_map : configMap.anchorSchemaMap // jshint ignore:line
    });

    $(window).bind('hashchange', _onHashChange).trigger('hashchange');

    var startupMap = $.uriAnchor.makeAnchorMap();

    if ( !startupMap.tab ) {
      changeAnchorPart({
        tab: configMap.defaultTab
      });
    }

    $.gevent.subscribe($(document), 'logging-updated', function() {
      var errors = blurconsole.model.logs.getLogs();
      if (errors.length === 0) {
        $('#view_logging_trigger .badge').html('');
      } else {
        $('#view_logging_trigger .badge').html(errors.length);
      }
    });
  }

  return {
    initModule: initModule,
    changeAnchorPart : changeAnchorPart
  };
}());