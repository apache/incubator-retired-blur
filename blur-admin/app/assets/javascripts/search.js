//= require jquery.dynatree

$(document).ready(function() {
  // PUSH STATE SEARCH QUERIES
  // When the history is popped add the query and table
  // then submit the query
  window.onpopstate = function(e) {
    if(e.state){
      $('#blur_table').val(e.state.table_id);
      $('#query_string').val(e.state.query);
      $('#search_submit').click();
    }
  };

  $('#search_form').on('submit', function(e) {
    // if the browser doesnt support push state return
    if (!Modernizr.history) {
      return;
    }

    // History state object
    var state = {
      table_id: $('#blur_table').find('option:selected').val(),
      query: $('#query_string').val()
    };

    // Build the new stateful query search
    var stateful_url = '?table_id=' + state.table_id + '&query=' + state.query;
    var base_url = location.protocol + "//" + location.host + location.pathname;
    var full_url = base_url + stateful_url;

    // If the length is zero it is the first search
    // and we want replace (avoid going back twice)
    if(location.search.length === 0){
      history.replaceState(state, "Search | Blur Console", full_url);
    // Otherwise we need to check that we arent searching
    // using the same query (dont push if it is the same)
    } else if(location.search !== stateful_url){
      history.pushState(state, "Search | Blur Console", full_url);
    }
  });

  /********** METHODS **********/
  var resizeSearch = function() {
    var footerHeight, headerHeight, resultWrapper;
    headerHeight = parseInt($('.navbar').css('height'));
    footerHeight = parseInt($('#ft').css('height'));
    resultWrapper = $('#results_wrapper');
    $('#results_wrapper').css('max-height', window.innerHeight - (footerHeight + headerHeight + parseInt(resultWrapper.css('margin-top')) + 50));
  };

  var hide_all_tabs = function() {
    $('.tab:visible').slideUp('fast');
    $('.arrow_up').hide();
    $('.arrow_down').show();
  };

  // method to initialize the filter tree
  var setup_filter_tree = function() {
    $('.column_family_filter').dynatree({
      checkbox: true,
      selectMode: 3,
      initAjax: get_filter_ajax()
    });
  };

  var get_filter_ajax = function() {
    return {
      url: Routes.filters_zookeeper_searches_path(CurrentZookeeper, $('#blur_table').val()),
      type: 'get'
    };
  };

  // Function to enable or disable submit button based on checkbox status
  var toggle_submit = function() {
    if ($(".column_family_filter").dynatree("getTree").getSelectedNodes().length > 0 && $('#query_string').val() !== '') {
      $('#search_submit').removeAttr('disabled');
      if ($('#save_name').val() !== '') {
        $('#save_button, #update_button').removeAttr('disabled');
      }
    } else {
      $('#save_button, #update_button, #search_submit').attr('disabled', 'disabled');
    }
  };

  /********** PAGE ACTIONS **********/
  // Setup the filters onload
  $.ui.dynatree.nodedatadefaults["icon"] = false;
  setup_filter_tree();
  $(window).resize(function() {
    var prevHeight;
    if (prevHeight !== window.innerHeight) {
      resizeSearch();
    }
    prevHeight = window.innerHeight;
  });

  /********** PAGE ELEMENT LISTENERS **********/
  // Reload the filters when the table selector is changed
  $('#blur_table').change(function() {
    var prevMode, tree;
    $(".column_family_filter").dynatree("option", "initAjax", get_filter_ajax());
    tree = $(".column_family_filter").dynatree("getTree");
    prevMode = tree.enableUpdate(false);
    tree.reload();
    tree.enableUpdate(prevMode);
  });

  // listener that checks if the submit button should be enabled on keystrokes
  $('#query_string, #save_name').live("keydown", function(name) {
    if (name.keyCode === 13 && !name.shiftKey) {
      return name.preventDefault();
    }
  });

  $('#query_string, #save_name').live("keyup", function(name) {
    var error_content;
    if (name.keyCode === 13 && !name.shiftKey) {
      name.preventDefault();
      if ($('#search_submit').attr('disabled')) {
        error_content = '<div style="color:red;font-style:italic; font-weight:bold">Invalid query search.</div>';
        $('#results_container').html(error_content);
        $('#results_wrapper').removeClass('hidden');
      } else {
        $('#search_form').submit();
      }
    } else {
      toggle_submit();
    }
  });

  // listener that accordion the filter sections
  $('.header').live('click', function() {
    if ($('.tab:visible').length > 0) {
      if ($(this).siblings('.tab:visible').length > 0) {
        $(this).siblings('.tab:visible').slideUp('fast');
        $(this).find('.arrow_up').hide();
        $(this).find('.arrow_down').show();
      } else {
        $('.tab').slideToggle('fast');
        $('.arrow').toggle();
      }
    } else {
      $(this).siblings('.body').slideDown('fast');
      $(this).find('.arrow_down').hide();
      $(this).find('.arrow_up').show();
    }
  });

  /********** more Functions **********/

  var fetch_error = function(error) {
    $('#results_container').html("<div class='no-results'>An error has occured: " + error + "</div>");
    $('#results_wrapper').addClass('noResults').removeClass('hidden');
  };

  var no_results = function() {
    $('#results_container').html('<div class="no-results">No results for your search.</div>');
    $('#results_wrapper').addClass('noResults').removeClass('hidden');
  };

  // disable buttons on load
  toggle_submit();
  //set up listeners for ajax to show spinner and disable buttons
  $('#loading-spinner').bind('ajaxStart', function() {
    $(this).removeClass('hidden');
  });
  $('#loading-spinner').bind('ajaxStop', function() {
    $(this).addClass('hidden');
  });
  $('#search_submit, #update_button, #save_button').bind('ajaxStart', function() {
    $(this).attr('disabled', 'disabled');
  });
  $('#search_submit, #update_button, #save_button').bind('ajaxStop', function() {
    toggle_submit();
  });
  $('body').live('click', function() {
    hide_all_tabs();
  });
  $('.tab:visible, .header').live('click', function(e) {
    return e.stopPropagation();
  });

  var populate_form = function(data) {
    var column, _i, _len, _ref;
    var oldTableName = $('#blur_table').val();
    $(".column_family_filter").dynatree("getRoot").visit(function(node) {
      return node.select(false);
    });
    $('#result_count').val(data.fetch);
    $('#offset').val(data.offset);
    $('#query_string').val(data.query);
    $('#save_name').val(data.name);
    //Checks to see if table still exists in hdfs before changing selector
    if ($('#blur_table').find('option[value='+ data.blur_table_id + ']').length != 0){
      $('#blur_table').val(data.blur_table_id);
    }
    /*
    *This seems backwards, but the .change trigger was not firing when jQuery was changing the dropdown selector.
    *This check forces the filter tree to refresh if the table changed
    */
    if (oldTableName != data.blur_table_id){
      var prevMode, tree;
      $(".column_family_filter").dynatree("option", "initAjax", get_filter_ajax());
      tree = $(".column_family_filter").dynatree("getTree");
      prevMode = tree.enableUpdate(false);
      tree.reload();
      tree.enableUpdate(prevMode);
    }

    if (data.super_query) {
      $('#search_row').prop('checked', true);
      $('#search_record').prop('checked', false);
      $('#return_row').prop('checked', true);
      $('#return_record').prop('checked', false).prop('disabled', true);
    } else if (data.record_only) {
      $('#search_row').prop('checked', false);
      $('#search_record').prop('checked', true);
      $('#return_row').prop('checked', false);
      $('#return_record').prop('checked', true).prop('disabled', false);
    } else {
      $('#search_row').prop('checked', false);
      $('#search_record').prop('checked', true);
      $('#return_row').prop('checked', true);
      $('#return_record').prop('checked', false).prop('disabled', false);
    }
    if (data.search_row) {
      $('#search_row').click();
    }
    if (data.search_record) {
      $('#search_record').click();
    }
    if (data.return_row) {
      $('#return_row').click();
    }
    if (data.return_record) {
      $('#return_record').click();
    }
    //check everything in the tree
    raw_columns = data.column_object;
    for(index = 0; index < raw_columns.length; index++){
      column = raw_columns[index];
      $(".column_family_filter").dynatree("getTree").selectKey(column);
    }

    $('#search_submit').removeAttr('disabled');
    $('#save_button').removeAttr('disabled');
    $('#update_button').removeAttr('disabled');
  };

  var retrieve_search = function(id) {
    $.ajax(Routes.load_zookeeper_search_path(CurrentZookeeper, id), {
      type: 'POST',
      success: function(data) {
        populate_form(data);
      }
    });
  };

   /********** PAGE AJAX LISTENERS **********/
   // fetch the results of a new search
  $('#search_form').submit(function() {
    $('#results_wrapper').addClass('noResults').removeClass('hidden');
    $('#results_wrapper').html('<div id="results_container"><div class="no-results">Loading...</div></div>');
    var form_data = $(this).serializeArray();
    var tree = $('.column_family_filter').dynatree('getTree');
    form_data = form_data.concat(tree.serializeArray());
    $.ajax(Routes.fetch_results_zookeeper_searches_path(CurrentZookeeper, $('#blur_table').val()), {
      data: form_data,
      type: 'post',
      success: function(data, status, xhr) {
        if (data) {
          $('#results_container').html(data);
          resizeSearch();
          $('#results_wrapper').removeClass('hidden noResults');
        } else {
          no_results();
        }
      },
     error: function(xhr, status, error) {
       fetch_error(error);
     }
    });
    return false;
  });

  // ajax listener for the edit action
  $('#edit_icon').live('click', function() {
    retrieve_search($(this).parents('.search_element').attr('id'));
    hide_all_tabs();
  });

  // ajax listener for the delete action
  $('#delete_icon').live('click', function() {
    var parent = $(this).parents('.search_element');
    var buttons = {
      "Delete Query": {
        "class": 'primary',
        func: function() {
          $().closePopup();
          $.ajax(Routes.delete_search_path(parent.attr("id"), $('#blur_table option:selected').val()), {
            type: 'DELETE',
            success: function(data) {
              $('#saved .body .saved').html(data);
            }
          });
        }
      },
    "Cancel": {
        func: function() {
          $().closePopup();
        }
      }
    };
    $().popup({
      btns: buttons,
      title: "Delete this saved query?",
      titleClass: 'title',
      body: "This will permanently delete the selected saved query. Do you wish to continue?"
    });
  });

  //ajax listener for the save action
  $('#save_button').live('click', function(evt) {
    var form_data = $('#search_form').serializeArray();
    var tree = $('.column_family_filter').dynatree('getTree');
    form_data = form_data.concat(tree.serializeArray());
    $.ajax(Routes.save_zookeeper_searches_path(CurrentZookeeper), {
      type: 'POST',
      data: form_data,
      success: function(data) {
        if (data) {
          $('#searches').replaceWith(data);
        }
      }
    });
  });

  //ajax listener for the update action
  $('#update_button').live('click', function(evt) {
    var match_found = false;
    var send_request = false;
    var search_id = "";
    //if the name in the "name" field matches a search then we can update
    $('.search_element').each(function(index, value) {
      if ($.trim($(value).children('.search-name').text()) === $.trim($('#save_name').val())) {
        //if we found another matching item do not send the update request
        if (match_found) {
          send_request = false;
        }
        send_request = true;
        match_found = true;
        search_id = $(value).attr('id');
      }
    });
    if (send_request) {
      var form_data = $('#search_form').serializeArray();
      var tree = $('.column_family_filter').dynatree('getTree');
      form_data = form_data.concat(tree.serializeArray());
      $.ajax(Routes.search_path(search_id), {
        type: 'PUT',
        data: form_data
      });
    } else {
      if (match_found) {
        var contentBody = "There are multiple saves with the same name.";
      } else {
        var contentBody = "There are no saved searches with that name.";
      }
      var message = "An error occurred while trying to update the saved search: " + contentBody + " To fix this error try changing the name.";
      return $().popup({
        title: 'Update Error',
        titleClass: 'title',
        body: message
      });
    }
  });

  //listener for the Search and Return Radiobuttons
  $('#search_row, #search_record, #return_row, #return_record').live('change', function(evt) {
    var search_row = $('#search_row');
    var search_record = $('#search_record');
    var return_row = $('#return_row');
    var return_record = $('#return_record');
    if (search_row[0] === $(this)[0]) {
      search_record.prop('checked', false);
      return_record.prop('checked', false);
      return_record.prop('disabled', true);
      return_row.prop('checked', true);
    } else if (search_record[0] === $(this)[0]) {
      search_row.prop('checked', false);
      return_record.prop('disabled', false);
    } else if (return_row[0] === $(this)[0]) {
      return_record.prop('checked', false);
    } else {
      return_row.prop('checked', false);
    }
  });
});


setTimeout(function() {
  var w = window.location;
  if (w.search.length > 1) {
    $('#search_submit').click();
  }
}, 1000);
