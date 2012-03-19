//= require jquery.dynatree
//= require bootstrap-tooltip
//= require bootstrap-popover

$(document).ready(function() {
  var refresh_timeout = null;
  var state_lookup = {
    0: 'deleted',
    1: 'deleting',
    2: 'disabled',
    3: 'disabling',
    4: 'active',
    5: 'enabling'
  };

  var table_lookup = {
    0: 'deleted',
    1: 'disabled',
    2: 'disabled',
    3: 'active',
    4: 'active',
    5: 'disabled'
  };

  var colspan_lookup = {
    'active': 6,
    'disabled': 4,
    'deleted': 2
  };

  var number_commas = function(number) {
    if (number) {
      return number.toString(10).replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
    } else {
      return 'Unknown';
    }
  };

  var get_selected_tables = function() {
    return $('.bulk-action-checkbox:checked').closest('tr').map(function() {
      return $(this).attr('blur_table_id');
    });
  };

  var get_host_shard_info = function(blur_table) {
    var server = $.parseJSON(blur_table['server']);
    if (server) {
      var hosts = 0;
      var count = 0;
      for (var key in server) {
        hosts++;
        count += server[key].length;
      }
      return {
        hosts: hosts,
        shards: count
      };
    } else {
      return {
        hosts: 'Unknown',
        shards: 'Unknown'
      };
    }
  };

  var build_table_row = function(blur_table) {
    var capitalize_first = function(string) {
      return string.charAt(0).toUpperCase() + string.slice(1);
    };

    var state = state_lookup[blur_table['status']];
    var table = table_lookup[blur_table['status']];
    var id = blur_table['id'];
    var host_info = get_host_shard_info(blur_table);
    var row = $("<tr class='blur_table' blur_table_id='" + id + "'></tr>");
    row.data('status', state);
    if (['disabling', 'enabling', 'deleting'].indexOf(state) >= 0) {
      var col_span = colspan_lookup[table];
      row.append("<td colspan='" + col_span + "'>" + (capitalize_first(state) + ' ' + blur_table['table_name']) + "...</td>");
    } else {
      var row_html = "<td class='checkbox-td'><input class='bulk-action-checkbox' type='checkbox'/><i class='icon-exclamation-sign queries-running-icon'></i>";
      row_html += "</td><td class='blur_table_name'>" + blur_table['table_name'] + "</td>";
      row.append(row_html);
      if (blur_table['has_queried_recently?']) {
        row.find('.queries-running-icon').show().addClass('icon-visible');
      } else {
        row.find('.queries-running-icon').hide().removeClass('icon-visible');
      }
      if (table === 'active') {
        var host_html = "<td class='blur_table_hosts_shards'>";
        if (blur_table['server']) {
          host_html += "<a class='hosts' href='" + (Routes.hosts_blur_table_path(id)) + "' data-remote='true'>" + host_info['hosts'] + " / " + host_info['shards'] + "</a>";
        } else {
          host_html += "Unknown";
        }
        row.append(host_html);
      }
      if (table !== 'deleted') {
        row.append("<td class='blur_table_row_count'>" + (number_commas(blur_table['row_count'])) + "</td>");
        row.append("<td class='blur_table_record_count'>" + (number_commas(blur_table['record_count'])) + "</td>");
      }
      if (table === 'active') {
        row.append("<td class='blur_table_info'><a class='info' href='" + (Routes.schema_blur_table_path(id)) + "' data-remote='true'>view</a></td>");
      }
      return row;
    }
  };

  var no_data = function() {
    $('.no-tables, .remove-next-update').remove();
    if (data.length === 0) {
      $('.check-all').prop('disabled', true).prop('checked', false);
      $('.bulk-action-button').addClass('suppress-button').each(function(idx, elm) {
        return $(elm).prop('disabled', true);
      });
      $('.blur_table').remove();
      var table_bodies = $('tbody');
      for (var index = 0; index < table_bodies.length; index++) {
        var tbody = table_bodies[index];
        var num_col = $(tbody).closest('table').find('th').length;
        $(tbody).append("<tr class='no-tables'><td/><td colspan='" + num_col + "'>No Tables Found</td></tr>");
      }
    }
  };

  var set_checkbox_state = function() {
    var tables = $('table');
    for (var index = 0; index < tables.length; index++) {
      var table = tables[index];
      var tbody = $(table).find('tbody');
      var number_of_tables = tbody.children().length;
      var number_of_checkboxes = tbody.find('.bulk-action-checkbox').length;
      var number_checked = tbody.find('.bulk-action-checkbox:checked:not(.check-all)').length;
      var tab_id = tbody.closest('div').attr('id');
      $("a[href=#" + tab_id + "] .counter").text(number_of_tables);
      if (number_of_tables === 0) {
        $(table).find('.check-all').prop('disabled', true).prop('checked', false);
        var num_col = tbody.closest('table').find('th').length;
        tbody.append("<tr class='no-tables'><td/><td colspan='" + num_col + "'>No Tables Found</td></tr>");
      } else {
        var check_all = $(table).find('.check-all').removeAttr('disabled');
        if (number_of_tables === number_checked) {
          check_all.prop('checked', true);
        } else {
          if (number_of_checkboxes === 0) check_all.prop('disabled', true);
          check_all.prop('checked', false);
        }
      }
    }
  };

  var set_cluster_alert = function() {
    var clusters = $('#blur_tables li');
    clusters.each(function(index) {
      var cluster_id = $(this).attr('id');
      cluster_id = parseInt(cluster_id.substring(cluster_id.length - 1), 10);
      if ($(".cluster[data-cluster_id='" + cluster_id + "']").find('.queries-running-icon.icon-visible').length > 0) {
        $(this).find('a .queries-running-icon').show();
      } else {
        $(this).find('a .queries-running-icon').hide();
      }
    });
  };

  var rebuild_table = function(data) {
    $('.no-tables, .remove-next-update').remove();
    if (data === null || data['tables'] === null || data['tables'].length === 0) {
      no_data();
    } else {
      if (data['clusters'] !== null && data['clusters'].length > 0) {
        var clusters = data['clusters'];
        for (var index = 0; index < clusters.length; index++) {
          var cluster = clusters[index];
          var safe_mode_icon = $('#cluster_tab_' + cluster['id'] + ' i.safemode-icon');
          if (cluster['safe_mode']) {
            safe_mode_icon.show();
          } else {
            safe_mode_icon.hide();
          }
        }
      }
      var currently_checked_rows = get_selected_tables();
      var tables = data['tables'];
      for (var index_tables = 0; index_tables < tables.length; index_tables++) {
        var blur_table = tables[index_tables];
        var selected_row = $('tr[blur_table_id=' + blur_table.id + ']');
        if (selected_row.length) {
          if (selected_row.data('status') !== state_lookup[blur_table['status']]) {
            selected_row.remove();
            var new_row_container = $("div#cluster_" + blur_table['cluster_id'] + "_" + table_lookup[blur_table['status']] + " table tbody");
            new_row_container.append(build_table_row(blur_table));
          }
          var properties_to_update = ['table_name', 'row_count', 'record_count'];
          for (var prop_index = 0; prop_index < properties_to_update.length; prop_index++) {
            var property = properties_to_update[prop_index];
            selected_row.find('blur_table_' + property).text(blur_table[property]);
          }
          var shard_info = get_host_shard_info(blur_table);
          if (blur_table['has_queried_recently?']) {
            selected_row.find('.queries-running-icon').show().addClass('icon-visible');
          } else {
            selected_row.find('.queries-running-icon').hide().removeClass('icon-visible');
          }
          if (blur_table['server']) {
            selected_row.find('blur_table_hosts_shards a').text(shard_info.hosts + "/" + shard_info.shards);
          } else {
            selected_row.find('blur_table_hosts_shards a').text("Unknown");
          }
        } else {
          new_row_container = $("div#cluster_" + blur_table['cluster_id'] + "_" + table_lookup[blur_table['status']] + " table tbody");
          new_row_container.append(build_table_row(blur_table));
        }
      }
      set_checkbox_state();
      set_cluster_alert();
    }
    refresh_timeout = setTimeout('window.reload_table_info()', 10000);
  };

  var reload_table_info = function() {
    $.get("" + (Routes.reload_blur_tables_path()), function(data) {
      rebuild_table(data);
    });
  };

  var row_highlight = function(should_highlight, table_row) {
    if (should_highlight) {
      table_row.addClass('highlighted-row');
    } else {
      table_row.removeClass('highlighted-row');
    }
  };

  var disable_action = function(table) {
    var checked = table.find('.bulk-action-checkbox:checked');
    var disabled = checked.length === 0;
    var actions = table.siblings('.btn');
    actions.prop('disabled', disabled);
    if (disabled) {
      actions.addClass('suppress-button');
    } else {
      actions.removeClass('suppress-button');
    }
  };

  var pending_change = function(cluster, tables, state, action) {
    for (var index = 0; index < tables.length; index++) {
      var table = tables[index];
      var blur_table = $("#cluster_" + cluster + "_" + state + " .cluster_table").find(".blur_table[blur_table_id='" + table + "']");
      var colspan = colspan_lookup[state];
      var table_name = blur_table.find('.blur_table_name').html();
      blur_table.removeClass('highlighted-row').children().remove();
      blur_table.append("<td colspan='" + colspan + "' class='remove-next-update'>" + action + " " + table_name + "...</td>");
    }
  };

  var get_terms = function(searchDiv, startWith) {
    var table_id = searchDiv.attr('table_id');
    var family = searchDiv.attr('family_name');
    var column = searchDiv.attr('column_name');
    if (typeof startWith === 'undefined') startWith = '';
    $.ajax({
      type: 'POST',
      url: Routes.terms_blur_table_path(table_id),
      data: {
        family: family,
        column: column,
        startWith: startWith,
        size: 21
      },
      dataType: 'json',
      success: function(data) {
        searchDiv.siblings('.terms-list').replaceWith(terms_to_list(data));
        if (data.length === 21) {
          searchDiv.siblings('.more-terms').attr('prev_term', data[data.length - 1]).children('.more-terms-btn').removeAttr('disabled');
        } else {
          searchDiv.siblings('.more-terms').children('.more-terms-btn').attr('disabled', 'disabled');
        }
      }
    });
  };

  var terms_to_list = function(terms) {
    var table_html = "<ul class='terms-list well'>";
    for (var index = 0; index < terms.length; index++) {
      var term = terms[index];
      table_html += "<li class='input-prepend term-li'><span class='add-on search-term-link' term='" + term + "'><i class='icon-search'></i></span><span class='input uneditable-input term-input'><span>" + term + "</span></span></li>";
    }
    return table_html += "</ul>";
  };

  var setup_filter_tree = function(selector) {
    return selector.dynatree();
  };

  window.reload_table_info = reload_table_info;
  reload_table_info();
  $.ui.dynatree.nodedatadefaults["icon"] = false;

  $('a.hosts, a.info').live('ajax:success', function(evt, data, status, xhr) {
    var title = $(this).attr('class');
    $(data).hide();
    $().popup({
      title: title.substring(0, 1).toUpperCase() + title.substring(1),
      titleClass: 'title',
      body: data,
      show: function(modal) {
        modal.children().hide();
        var popup_tree = $(modal).children('.modal-body').find('.' + title);
        if (popup_tree.size() > 0) setup_filter_tree(popup_tree);
        modal.children().show();
      }
    });
  });

  $('.check-all').live('change', function() {
    var checked = $(this).is(':checked');
    $(this).prop('checked', checked);
    var boxes = $(this).closest('table').children('tbody').find('.bulk-action-checkbox');
    boxes.each(function(idx, box) {
      $(box).prop('checked', checked);
      row_highlight(checked, $(this).parents('.blur_table'));
    });
  });

  $('.bulk-action-checkbox').live('change', function() {
    var cluster_table = $(this).closest('table');
    if (!$(this).hasClass('check-all')) {
      cluster_table.find('.check-all').prop('checked', false);
    }
    var table_row = $(this).parents('.blur_table');
    if (table_row.length === 1) row_highlight($(this).is(':checked'), table_row);
    disable_action(cluster_table);
    var num_checked = cluster_table.find('.bulk-action-checkbox:checked').length;
    if (num_checked === cluster_table.find('tbody tr .bulk-action-checkbox').length) {
      cluster_table.find('.check-all').prop('checked', true);
    }
  });

  $('.btn').live('click', function() {
    var btns, msg, title;
    var action = $(this).attr('blur_bulk_action');
    if (!action) return;
    var cluster_table = $(this).siblings('table');
    var cluster_id = cluster_table.attr('blur_cluster_id');
    var blur_tables = cluster_table.children('tbody').children('.blur_table');
    var checked_tables = blur_tables.filter('.highlighted-row');
    var table_ids = new Array();
    blur_tables.each(function(index, element) {
      if ($(element).children('td').children('input[type="checkbox"]').is(':checked')) {
        table_ids.push($(element).attr('blur_table_id'));
      }
    });
    if (table_ids.length <= 0) return;
    var sharedAjaxSettings = {
      beforeSend: function() {
        clearTimeout(refresh_timeout);
      },
      success: function(data) {
        if (action === 'forget') checked_tables.remove();
        rebuild_table(data);
      },
      data: {
        tables: table_ids,
        cluster_id: cluster_id
      }
    };
    switch (action) {
      case 'enable':
        btns = {
          "Enable": {
            "class": "primary",
            func: function() {
              $.extend(sharedAjaxSettings, {
                type: 'PUT',
                url: Routes.enable_selected_blur_tables_path()
              });
              $.ajax(sharedAjaxSettings);
              pending_change(cluster_id, table_ids, 'disabled', 'Enabling');
              $().closePopup();
            }
          },
          "Cancel": {
            func: function() {
              $().closePopup();
            }
          }
        };
        title = "Enable Tables";
        msg = "Are you sure you want to enable these tables?";
        break;
      case 'disable':
        btns = {
          "Disable": {
            "class": "primary",
            func: function() {
              $.extend(sharedAjaxSettings, {
                type: 'PUT',
                url: Routes.disable_selected_blur_tables_path()
              });
              $.ajax(sharedAjaxSettings);
              pending_change(cluster_id, table_ids, 'active', 'Disabling');
              $().closePopup();
            }
          },
          "Cancel": {
            func: function() {
              $().closePopup();
            }
          }
        };
        title = "Disable Tables";
        msg = "Are you sure you want to disable these tables?";
        break;
      case 'forget':
        btns = {
          "Forget": {
            "class": "primary",
            func: function() {
              $.extend(sharedAjaxSettings, {
                type: 'DELETE',
                url: Routes.forget_selected_blur_tables_path()
              });
              $.ajax(sharedAjaxSettings);
              pending_change(cluster_id, table_ids, 'deleted', 'Forgetting');
              $().closePopup();
            }
          },
          "Cancel": {
            func: function() {
              $().closePopup();
            }
          }
        };
        title = "Forget Tables";
        msg = "Are you sure you want to forget these tables?";
        break;
      case 'delete':
        var delete_tables = function(delete_index) {
          $.extend(sharedAjaxSettings, {
            type: 'DELETE',
            url: Routes.destroy_selected_blur_tables_path()
          });
          sharedAjaxSettings.data.delete_index = delete_index;
          $.ajax(sharedAjaxSettings);
          pending_change(cluster_id, table_ids, 'disabled', 'Deleting');
        };
        btns = {
          "Delete tables and indicies": {
            "class": "danger",
            func: function() {
              delete_tables(true);
              $().closePopup();
            }
          },
          "Delete tables only": {
            "class": "warning",
            func: function() {
              delete_tables(false);
              $().closePopup();
            }
          },
          "Cancel": {
            func: function() {
              $().closePopup();
            }
          }
        };
        title = "Delete Tables";
        msg = 'Do you want to delete all of the underlying table indicies?';
        break;
      default:
        return;
    }
    $().popup({
      title: title,
      titleClass: 'title',
      body: msg,
      btns: btns
    });
  });

  /*
    Terms functions
  */

  $('.terms').live('click', function(evt) {
    var term = $(this);
    var table_id = $(this).attr('table_id');
    var family = $(this).attr('family_name');
    var column = $(this).attr('column_name');
    var content = "<div id='new-popover-search' class='form-search' table_id='" + table_id + "' family_name='" + family + "' column_name='" + column + "'><input type='search' class='term-search span2' placeholder='Search...'/><a class='btn btn-primary term-search-btn'>Search</a></div>";
    content += "<ul class='terms-list'><li>Loading...</li></ul><div class='more-terms btn-group'><a href='#' class='more-terms-btn btn btn-primary'>More...</a><a href='#' class='reset-terms btn'><i class='icon-refresh'></i></a></div>";
    term.popover({
      title: column + " terms<i class='icon-remove popover-close' style='position:absolute; top:15px;right:15px'></i>",
      content: content,
      trigger: 'focus',
      placement: 'right'
    });
    term.popover('show');
    var newPopover = $('#new-popover-search');
    get_terms(newPopover);
    newPopover.removeAttr('id').parents('.popover').css('top', '0px').children('.arrow').remove();
  });

  $('.more-terms-btn').live('click', function(evt) {
    var searchDiv = $(this).parent().siblings('.form-search');
    var prevTerm = $(this).parent().attr('prev_term');
    get_terms(searchDiv, prevTerm);
  });

  $('.term-search').live('keydown', function(evt) {
    if (evt.which === 13) {
      evt.preventDefault();
      $(this).siblings('.term-search-btn').click();
    }
  });

  $('.term-search-btn').live('click', function(evt) {
    var btn = $(this);
    var searchDiv = btn.parent();
    var startWith = btn.siblings('.term-search').val();
    get_terms(searchDiv, startWith);
  });

  $('.reset-terms').live('click', function(evt) {
    var btn = $(this);
    var searchDiv = btn.parent().siblings('.form-search');
    get_terms(searchDiv);
    searchDiv.children('.term-search').val('');
  });

  $('.popover-close').live('click', function(evt) {
    $(this).parents('.popover').remove();
  });

  $('.search-term-link').live('click', function(evt) {
    var searchDiv = $(this).parents('.terms-list').siblings('.form-search');
    var term = $(this).attr('term');
    var table_id = searchDiv.attr('table_id');
    var family = searchDiv.attr('family_name');
    var column = searchDiv.attr('column_name');
    window.location = Routes.search_path() + ("?table_id=" + table_id + "&query=") + encodeURIComponent("" + family + "." + column + ":" + term);
  });
});