//= require jquery.dynatree

$(document).ready(function() {
  var fetch_error, get_filter_ajax, hide_all_tabs, no_results, populate_form, resizeSearch, retrieve_search, setup_filter_tree, toggle_submit;
  
  /********** METHODS **********/
  resizeSearch = function() {
    var footerHeight, headerHeight, resultWrapper;
    headerHeight = parseInt($('.navbar').css('height'));
    footerHeight = parseInt($('#ft').css('height'));
    resultWrapper = $('#results_wrapper');
    return $('#results_wrapper').css('max-height', window.innerHeight - (footerHeight + headerHeight + parseInt(resultWrapper.css('margin-top')) + 50));
  };
  
  hide_all_tabs = function() {
    $('.tab:visible').slideUp('fast');
    $('.arrow_up').hide();
    return $('.arrow_down').show();
  };
  
  // method to initialize the filter tree
  setup_filter_tree = function() {
    return $('.column_family_filter').dynatree({
      checkbox: true,
      selectMode: 3,
      initAjax: get_filter_ajax()
    });
  };
  
  get_filter_ajax = function() {
    var options;
    return options = {
      url: Routes.filters_zookeeper_searches_path(CurrentZookeeper, $('#blur_table').val()),
      type: 'get'
    };
  };
  
  // Function to enable or disable submit button based on checkbox status
  toggle_submit = function() {
    if ($(".column_family_filter").dynatree("getTree").getSelectedNodes().length > 0 && $('#query_string').val() !== '') {
      $('#search_submit').removeAttr('disabled');
      if ($('#save_name').val() !== '') {
        return $('#save_button, #update_button').removeAttr('disabled');
      }
    } else {
      return $('#save_button, #update_button, #search_submit').attr('disabled', 'disabled');
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
    return prevHeight = window.innerHeight;
  });
  
  /********** PAGE ELEMENT LISTENERS **********/
  // Reload the filters when the table selector is changed
  $('#blur_table').change(function() {
    var prevMode, tree;
    $(".column_family_filter").dynatree("option", "initAjax", get_filter_ajax());
    tree = $(".column_family_filter").dynatree("getTree");
    prevMode = tree.enableUpdate(false);
    tree.reload();
    return tree.enableUpdate(prevMode);
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
        return $('#results_wrapper').removeClass('hidden');
      } else {
        return $('#search_form').submit();
      }
    } else {
      return toggle_submit();
    }
  });
  
  // listener that accordion the filter sections
  $('.header').live('click', function() {
    if ($('.tab:visible').length > 0) {
      if ($(this).siblings('.tab:visible').length > 0) {
        $(this).siblings('.tab:visible').slideUp('fast');
        $(this).find('.arrow_up').hide();
        return $(this).find('.arrow_down').show();
      } else {
        $('.tab').slideToggle('fast');
        return $('.arrow').toggle();
      }
    } else {
      $(this).siblings('.body').slideDown('fast');
      $(this).find('.arrow_down').hide();
      return $(this).find('.arrow_up').show();
    }
  });
  
  /********** more Functions **********/
  
  fetch_error = function(error) {
    $('#results_container').html("<div class='no-results'>Anerror has occured: " + error + "</div>");
    return $('#results_wrapper').addClass('noResults').removeClass('hidden');
  };
  
  no_results = function() {
    $('#results_container').html('<div class="no-results">No results for your search.</div>');
    return $('#results_wrapper').addClass('noResults').removeClass('hidden');
  };
  
  // disable buttons on load
  toggle_submit();
  //set up listeners for ajax to show spinner and disable buttons
  $('#loading-spinner').bind('ajaxStart', function() {
    return $(this).removeClass('hidden');
  });
  $('#loading-spinner').bind('ajaxStop', function() {
    return $(this).addClass('hidden');
  });
  $('#search_submit, #update_button, #save_button').bind('ajaxStart', function() {
    return $(this).attr('disabled', 'disabled');
  });
  $('#search_submit, #update_button, #save_button').bind('ajaxStop', function() {
    return toggle_submit();
  });
  $('body').live('click', function() {
    return hide_all_tabs();
  });
  $('.tab:visible, .header').live('click', function(e) {
    return e.stopPropagation();
  });
  
  populate_form = function(data) {
    var column, _i, _len, _ref;
    $(".column_family_filter").dynatree("getRoot").visit(function(node) {
      return node.select(false);
    });
    $('#result_count').val(data.fetch);
    $('#offset').val(data.offset);
    $('#query_string').val(data.query);
    $('#save_name').val(data.name);
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
    _ref = data.column_object;
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      column = _ref[_i];
      $(".column_family_filter").dynatree("getTree").selectKey(column);
    }
    $('#search_submit').removeAttr('disabled');
    $('#save_button').removeAttr('disabled');
    return $('#update_button').removeAttr('disabled');
  };
  
  retrieve_search = function(id) {
    return $.ajax(Routes.load_zookeeper_search_path(CurrentZookeeper, id), {
      type: 'POST',
      success: function(data) {
        return populate_form(data);
      }
    });
  };
  
   /********** PAGE AJAX LISTENERS **********/
   // fetch the results of a new search
  $('#search_form').submit(function() {
    var form_data, tree;
    form_data = $(this).serializeArray();
    tree = $('.column_family_filter').dynatree('getTree');
    form_data = form_data.concat(tree.serializeArray());
    $.ajax(Routes.fetch_results_zookeeper_searches_path(CurrentZookeeper, $('#blur_table').val()), {
      data: form_data,
      type: 'post',
      success: function(data, status, xhr) {
        if (data) {
          $('#results_container').html(data);
          resizeSearch();
          $('#results_wrapper').removeClass('hidden noResults');
        }else 
          no_results();
      },
     error: function(xhr, status, error) {
       return fetch_error(error);
     }
    });
    return false;
  });
  
  // ajax listener for the edit action
  $('#edit_icon').live('click', function() {
    return retrieve_search($(this).parents('.search_element').attr('id'));
  });
  
  // ajax listener for the delete action
  $('#delete_icon').live('click', function() {
    var buttons, parent;
    parent = $(this).parents('.search_element');
    buttons = {
      "Delete Query": {
        "class": 'primary',
        func: function() {
          $().closePopup();
          return $.ajax(Routes.delete_zookeeper_search_path(CurrentZookeeper, parent.attr("id"), $('#blur_table option:selected').val()), {
            type: 'DELETE',
            success: function(data) {
              return $('#saved .body .saved').html(data);
            }
          });
        }
      }
    };
    ({
      "Cancel": {
        func: function() {
          return $().closePopup();
        }
      }
    });
    return $().popup({
      btns: buttons,
      title: "Delete this saved query?",
      titleClass: 'title',
      body: "This will permanently delete the selected saved query. Do you wish to continue?"
    });
  });
  
  //ajax listener for the save action
  $('#save_button').live('click', function(evt) {
    var form_data, tree;
    form_data = $('#search_form').serializeArray();
    tree = $('.column_family_filter').dynatree('getTree');
    form_data = form_data.concat(tree.serializeArray());
    return $.ajax(Routes.save_zookeeper_searches_path(CurrentZookeeper), {
      type: 'POST',
      data: form_data,
      success: function(data) {
        if (data) {
          return $('#searches').replaceWith(data);
        }
      }
    });
  });
  
  //ajax listener for the update action
  $('#update_button').live('click', function(evt) {
    var contentBody, form_data, match_found, message, search_id, send_request, tree;
    match_found = false;
    send_request = false;
    search_id = "";
    //if the name in the "name" field matches a search then we can update
    $('.search_element').each(function(index, value) {
      if ($.trim($(value).children('.search-name').text()) === $.trim($('#save_name').val())) {
        //if we found another matching item do not send the update request
        if (match_found) {
          return send_request = false;
        }
        send_request = true;
        match_found = true;
        return search_id = $(value).attr('id');
      }
    });
    if (send_request) {
      form_data = $('#search_form').serializeArray();
      tree = $('.column_family_filter').dynatree('getTree');
      form_data = form_data.concat(tree.serializeArray());
      return $.ajax(Routes.zookeeper_search_path(CurrentZookeeper, search_id), {
        type: 'PUT',
        data: form_data
      });
    } else {
      if (match_found) {
        contentBody = "There are multiple saves with the same name.";
      } else {
        contentBody = "There are no saved searches with that name.";
      }
      message = "An error occurred while trying to update the saved search: " + contentBody + " To fix this error try changing the name.";
      return $().popup({
        title: 'Update Error',
        titleClass: 'title',
        body: message
      });
    }
  });
  
  //listener for the Search and Return Radiobuttons
  return $('#search_row, #search_record, #return_row, #return_record').live('change', function(evt) {
    var rr, rrec, sr, srec;
    sr = $('#search_row');
    srec = $('#search_record');
    rr = $('#return_row');
    rrec = $('#return_record');
    if (sr[0] === $(this)[0]) {
      srec.prop('checked', false);
      rrec.prop('checked', false);
      rrec.prop('disabled', true);
      return rr.prop('checked', true);
    } else if (srec[0] === $(this)[0]) {
      sr.prop('checked', false);
      return rrec.prop('disabled', false);
    } else if (rr[0] === $(this)[0]) {
      return rrec.prop('checked', false);
    } else {
      return rr.prop('checked', false);
    }
  });
});


setTimeout(function() {
  var w = window.location;
  if (w.search.length > 1) {
    return $('#search_submit').click();
  }
}, 1000);
