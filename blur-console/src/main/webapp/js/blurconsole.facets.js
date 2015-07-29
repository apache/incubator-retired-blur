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
blurconsole.facets = (function () {
  'use strict';

    //----------------------------- Configuration and State -------------------------
  var
    configMap = {
      mainHtml: String()
        + '<div class="container-fluid">'
          + '<div class="alert alert-danger facetWarning" style="display:none">Please make sure you select a family, column and add terms to run faceting.</div>'
          + '<div class="row">'
            + '<form>'
              + '<div class="col-md-6">'
                + '<div class="form-group">'
                  + '<label for="facetFamily">Family</label>'
                  + '<select class="form-control" id="facetFamily">'
                  + '</select>'
                + '</div>'
                + '<div class="form-group">'
                  + '<label for="facetColumn">Column</label>'
                  + '<select class="form-control" id="facetColumn">'
                  + '</select>'
                + '</div>'
              + '</div>'
              + '<div class="col-md-6">'
                + '<div class="form-group">'
                  + '<label>Terms</label>'
                  + '<span id="facetTerms"></span>'
                  // + '<input type="text" name="facetTerms" class="form-control tm-input" id="facetTerms" autocomplete="off"/>'
                  + '<div class="facetTermList"></div>'
                + '</div>'
                + '<div class="form-group">'
                  + '<button type="button" class="btn btn-primary" id="facetSubmit">Run</button>'
                + '</div>'
              + '</div>'
            + '</form>'
          + '</div>'
          + '<hr/>'
          + '<div class="row">'
            + '<div class="col-md-12 facetResults">'
            + '</div>'
          + '</div>'
        + '</div>'
    },
    stateMap = {},
    jqueryMap = {};

  //------------------------------ Private Methods -----------------------------------------------------
  function _switchToSearch(evt){
    var family = jqueryMap.familyChooser.val();
    var column = jqueryMap.columnChooser.val();
    var initialQuery = stateMap.query;
    blurconsole.shell.changeAnchorPart({
      tab: 'search',
      _tab: {
        query: encodeURIComponent('+(' + initialQuery + ') +(+' + family + '.' + column + ':(' + $(evt.currentTarget).data('term') + '))'),
        table: stateMap.table,
        rr: 'rowrow'
      }
    });
    jqueryMap.modal.modal('hide');
  }

  //------------------------------ Event Handling and DOM Methods --------------------------------------
  function _showFacet(event, data) {
    stateMap.table = data.table;
    stateMap.query = data.query;
    stateMap.modalId = stateMap.table + '_modal';
    blurconsole.model.tables.getSchema(stateMap.table, _popupFacetView);
  }

  function _popupFacetView(schema) {
    stateMap.schema = schema;
    jqueryMap.contentHolder = $(configMap.mainHtml);
    jqueryMap.contentHolder.find('#facetFamily').html(_buildFamilyOptions());

    jqueryMap.modal = $(blurconsole.browserUtils.modal(stateMap.modalId, 'Faceting for ' + stateMap.query, jqueryMap.contentHolder, null, 'large'));
    jqueryMap.modal.modal()
    .on('shown.bs.modal', function(){
      jqueryMap.familyChooser = $('#facetFamily', jqueryMap.modal);
      jqueryMap.columnChooser = $('#facetColumn', jqueryMap.modal);
      jqueryMap.facetResults = $('.facetResults', jqueryMap.modal);
      jqueryMap.terms = $('#facetTerms', jqueryMap.modal);

      jqueryMap.terms.selectivity({
        inputType: 'Email',
        placeholder: 'Enter in terms'
      });

      $('#facetSubmit', jqueryMap.modal).on('click', _runFacetCounts);
    })
    .on('hidden.bs.modal', function(e) {
      $(e.currentTarget).remove();
      jqueryMap.contentHolder.remove();
      jqueryMap = {};
      stateMap = {};
    })
    .on('change', 'select#facetFamily', function(e) {
      var family = $(e.currentTarget).val();
      jqueryMap.columnChooser.html(_buildColumnOptions(family));
      return false;
    })
    .on('click', '.searchTrigger', _switchToSearch);
  }

  function _buildFamilyOptions() {
    var options = '<option value="">Select a Family</option>';

    $.each(stateMap.schema, function(family) {
      options += '<option value="' + family + '">' + family + '</option>';
    });
    return options;
  }

  function _buildColumnOptions(family) {
    var options = '<option value="">Select a Column</option>';

    $.each(stateMap.schema[family], function(column) {
      options += '<option value="' + column + '">' + column + '</option>';
    });
    return options;
  }

  function _runFacetCounts() {
    var family = jqueryMap.familyChooser.val();
    var column = jqueryMap.columnChooser.val();
    var terms = jqueryMap.terms.selectivity('value');

    if (family && column && terms.length > 0) {
      $('.facetWarning', jqueryMap.modal).hide();
      blurconsole.model.search.runFacetCount(stateMap.query, stateMap.table, family, column, terms, _displayFacetCounts);
    } else {
      $('.facetWarning', jqueryMap.modal).show();
    }
  }

  function _displayFacetCounts(counts) {
    var markup = '<table class="table table-condensed table-hover table-bordered"><thead><tr><th>Term</th><th>Count</th><th></th></thead><tbody>';

    $.each(counts, function(term, count){
      markup += '<tr>';
      markup += '<td>' + term + '</td>';
      markup += '<td>' + count + '</td>';
      markup += '<td><button type="button" class="btn btn-sm btn-default searchTrigger" title="Run search with facet added" data-term="' + term + '"><i class="glyphicon glyphicon-search"/></button></td>';
      markup += '</tr>';
    });

    markup += '</tbody></table>';

    jqueryMap.facetResults.html(markup);
  }

    //----------------------------- Public API ----------------------------
  function initModule() {
    $.gevent.subscribe($(document), 'facet-show', _showFacet);
  }

  return {
    initModule : initModule
  };
}());