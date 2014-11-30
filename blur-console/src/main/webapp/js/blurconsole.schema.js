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
/*jshint laxbreak: true */
/*global blurconsole:false */
blurconsole.schema = (function () {
  'use strict';

    //----------------------------- Configuration and State -------------------------
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
    jqueryMap = {};

  //------------------------------ Private Methods -----------------------------------------------------
  function _findTerms() {
    blurconsole.model.tables.findTerms(stateMap.table, stateMap.termFamily, stateMap.termColumn, jqueryMap.termSearch.val(), _loadTerms);
  }

  //------------------------------ Event Handling and DOM Methods --------------------------------------
  function _showSchema(event, table) {
    stateMap.table = table;
    stateMap.modalId = stateMap.table + '_modal';
    blurconsole.model.tables.getSchema(stateMap.table, _popupSchemaView);
  }

  function _popupSchemaView(schema) {
    stateMap.schema = schema;
    jqueryMap.contentHolder = $(configMap.mainHtml);
    jqueryMap.contentHolder.find('.schemaList').html(_buildTreeSection());
    jqueryMap.contentHolder.find('.schemaColumnInfo').append(_buildInfoSection());

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
    .on('click', 'a.termsTrigger', _viewTerms)
    .on('click', '.schemaColumnTerms button', _findTerms)
    .on('click', '.searchTrigger', _switchToSearch);
  }

  function _buildTreeSection() {
    var tree = '';
    $.each(stateMap.schema, function(family, cols){
      var famId = blurconsole.browserUtils.cleanId(family);
      tree += '<div class="panel panel-default"><div class="panel-heading">';
      tree += '<h4 class="panel-title" data-toggle="collapse" data-parent=".schemaList" data-target="#' + famId + '">' + family + '</h4></div>';
      tree += '<div id="' + famId + '" class="panel-collapse collapse"><div class="panel-body"><ul class="list-group">';
      $.each(cols, function(col, def) {
        var colId = blurconsole.browserUtils.cleanId(col);
        tree += '<li class="list-group-item schemaColumn"><a href="#' + famId + '_' + colId + '">' + col + '';
        tree += '</a><div class="pull-right">';
        if (def.type !== 'stored') {
          tree += ' <span class="badge">searchable</span>';
        }
        tree += '<i class="glyphicon glyphicon-chevron-right"></i></div></li>';
      });
      tree += '</ul></div></div></div>';
    });
    return tree;
  }

  function _buildInfoSection() {
    var info = '';
    $.each(stateMap.schema, function(family, cols){
      var famId = blurconsole.browserUtils.cleanId(family);
      $.each(cols, function(col, def){
        var colId = blurconsole.browserUtils.cleanId(col);
        info += '<div class="schemaColumnDef" id="' + famId + '_' + colId + '"><ul class="list-group">';
        info += '<li class="list-group-item"><strong>Field Name:</strong> ' + col + '</li>';
        info += '<li class="list-group-item"><strong>Fieldless Searching:</strong> ' + blurconsole.browserUtils.booleanImg(def.fieldLess) + '</li>';
        info += '<li class="list-group-item"><strong>Field Type:</strong> ' + def.type + '</li>';
        if (def.extra) {
          $.each(def.extra, function(key, value) {
            info += '<li class="list-group-item"><strong>' + key + ':</strong> ' + value + '</li>';
          });
        }
        if (def.type !== 'stored' && blurconsole.auth.hasRole('searcher')) {
          info += '<li class="list-group-item"><a href="#" class="termsTrigger" data-fam="' + family + '" data-col="' + col + '">View Terms</a></li>';
        }
        info += '</ul></div>';
      });
    });
    return info;
  }

  function _viewTerms(evt) {
    jqueryMap.termList.html('<div class="center-block"><img src="img/ajax-loader.gif"></div>');
    jqueryMap.termSearch.val('');
    jqueryMap.columnTermsSection.show();
    var $this = $(evt.currentTarget);

    stateMap.termFamily = $this.data('fam');
    stateMap.termColumn = $this.data('col');

    jqueryMap.termSearchButton.trigger('click');
  }

  function _loadTerms(terms) {
    jqueryMap.termList.html('');
    $.each(terms, function(i, term){
      jqueryMap.termList.append('<li class="list-group-item">' + term + ' <span class="badge badge-success searchTrigger" title="Search for this value" data-value="' + term + '" data-table="' + stateMap.table + '"><i class="glyphicon glyphicon-search"></i></span></li>');
    });
  }

  function _switchToSearch(evt){
    blurconsole.shell.changeAnchorPart({
      tab: 'search',
      _tab: {
        query: encodeURIComponent(stateMap.termFamily + '.' + stateMap.termColumn + ':' + $(evt.currentTarget).data('value')),
        table: $(evt.currentTarget).data('table'),
        rr: 'rowrow'
      }
    });
    jqueryMap.modal.modal('hide');
  }

    //----------------------------- Public API ----------------------------
  function initModule() {
    $.gevent.subscribe($(document), 'schema-show', _showSchema);
  }

  return {
    initModule : initModule
  };
}());