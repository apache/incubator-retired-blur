//= require jquery.dataTables
//= require datatables.fnReloadAjax
//= require_self

$.extend($.fn.dataTableExt.oStdClasses, {
  "sSortAsc": "header headerSortDown",
  "sSortDesc": "header headerSortUp",
  "sSortable": "header"
});

$(document).ready(function() {
  var visible_column_count = $('#queries-table thead th').length;
  var refresh_rate = -1;
  var refresh_timeout = null;
  var data_table = null;
  var load_queries = function() {
    data_table = $('#queries-table').dataTable({
      "sDom": "<'row'<'span4'i><'span2'r><'span3'f>>t",
      bPaginate: false,
      bProcessing: true,
      bAutoWidth: false,
      bDeferRender: true,
      "oLanguage": {
        "sInfoEmpty": "",
        "sInfo": "Displaying _TOTAL_ queries",
        "sSearch": "Filter queries:",
        "sZeroRecords": "No queries to display",
        "sInfoFiltered": "(filtered from _MAX_ total queries)"
      },
      sAjaxSource: Routes.refresh_zookeeper_blur_queries_path(CurrentZookeeper, 1),
      aoColumns: table_cols(),
      fnRowCallback: process_row
    });
    add_refresh_rates(data_table);
    $('#queries-table').ajaxComplete(function(e, xhr, settings) {
      if (settings.url.indexOf('/blur_queries/refresh') >= 0) {
        if (refresh_rate > -1) {
          refresh_timeout = setTimeout(function() {
            var range_time_limit = $('.time_range').find('option:selected').val();
            data_table.fnReloadAjax(Routes.refresh_zookeeper_blur_queries_path(CurrentZookeeper, range_time_limit));
          }, refresh_rate * 1000);
        }
      }
    });
    $('.time_range').live('change', function() {
      var range_time_limit = $(this).find('option:selected').val();
      data_table.fnReloadAjax(Routes.refresh_zookeeper_blur_queries_path(CurrentZookeeper, range_time_limit));
    });
  };
  var table_cols = function() {
    if (visible_column_count === 8) {
      return [
        {
          "mDataProp": "userid"
        }, {
          "mDataProp": "query",
          "sWidth": "500px"
        }, {
          "mDataProp": "tablename"
        }, {
          "mDataProp": "start"
        }, {
          "mDataProp": "time"
        }, {
          "mDataProp": "status",
          "sWidth": "150px"
        }, {
          "mDataProp": "state",
          "bVisible": false
        }, {
          "mDataProp": "action"
        }
      ];
    }
    return [
      {
        "mDataProp": "userid"
      }, {
        "mDataProp": "tablename"
      }, {
        "mDataProp": "start"
      }, {
        "mDataProp": "time"
      }, {
        "mDataProp": "status",
        "sWidth": "150px"
      }, {
        "mDataProp": "state",
        "bVisible": false
      }, {
        "mDataProp": "action"
      }
    ];
  };
  var process_row = function(row, data, rowIdx, dataIdx) {
    var action_td = $('td:last-child', row);
    if (action_td.html() === '') {
      action_td.append("<a href='" + (Routes.more_info_zookeeper_blur_query_path(CurrentZookeeper, data['id'])) + "' class='more_info' style='margin-right: 3px'>More Info</a>");
      if (data['state'] === 'Running' && data['can_update']) {
        action_td.append("<form accept-charset='UTF-8' action='" + (Routes.zookeeper_blur_query_path(CurrentZookeeper, data['id'])) + "' class='cancel' data-remote='true' method='post'><div style='margin:0;padding:0;display:inline'><input name='_method' type='hidden' value='put'></div><input id='cancel' name='cancel' type='hidden' value='true'><input class='cancel_query_button btn btn-small' type='submit' value='Cancel'><img src='/assets/loading.gif' style='display:none'></form>");
      }
    }
    var time = data.time.substring(0, data.time.indexOf(' ')).split(':');
    var timeModifier = data.time.substring(data.time.indexOf(' ') + 1) === 'PM';
    var timeInSecs = (timeModifier ? parseInt(time[0], 10) + 12 : parseInt(time[0], 10)) * 3600 + parseInt(time[1], 10) * 60 + parseInt(time[2], 10);
    var dateNow = new Date();
    var timeNowSecs = dateNow.getHours() * 3600 + dateNow.getMinutes() * 60 + dateNow.getSeconds();
    if (data.state === 'Running' && Math.abs(timeNowSecs - timeInSecs) > 60) {
      $(row).addClass('oldRunning');
    }
    return row;
  };
  var add_refresh_rates = function(data_table) {
    var refresh_content = '<div class="span3">Auto Refresh: <div class="btn-group">';
    var options = [
      {
        'key': 'Off',
        'value': -1
      }, {
        'key': '10s',
        'value': 10
      }, {
        'key': '1m',
        'value': 60
      }, {
        'key': '10m',
        'value': 600
      }
    ];
    $.each(options, function(idx, val) {
      var link_class = idx === 0 ? 'btn-primary' : '';
      refresh_content += "<a href='javascript:void(0)' class='refresh_option " + link_class + " btn' data-refresh_val='" + val.value + "'>" + val.key + "</a>";
    });
    refresh_content += '</div></div>';
    $('#queries-table_wrapper > .row:first-child').prepend(refresh_content);
    $('.dataTables_wrapper .row .span3:first-child .btn-group').append('<a id="refresh-queries" class="btn"><i class="icon-refresh"/></a>');
    $('#refresh-queries').click(function() {
      if ($(this).attr('disabled') !== 'disabled'){
        data_table.fnReloadAjax();
      }
    });
    $('a.refresh_option').click(function() {
      $('a.refresh_option').removeClass('btn-primary');
      $(this).addClass('btn-primary');
      var prev_refresh_rate = refresh_rate;
      refresh_rate = $(this).data('refresh_val');
      if (prev_refresh_rate === -1) {
        data_table.fnReloadAjax();
      }
      else if (refresh_rate === -1 && refresh_timeout)
      {
        clearTimeout(refresh_timeout);
      }
      if (refresh_rate === -1) {
        $('#refresh-queries').removeAttr('disabled');
      }
      else
      {
        $('#refresh-queries').attr('disabled', 'disabled');
      }
    });
  };
  var truncate = function(value, length, ommission) {
    if (!value) return null;
    if (!(value.length > length)) return value;
    return "" + (value.substring(0, length)) + (ommission != null ? ommission : {
      ommission: ''
    });
  };
  $('.more_info').live('click', function(e) {
    e.preventDefault();
    $.ajax({
      url: $(this).attr('href'),
      type: 'GET',
      success: function(data) {
        $().popup({
          title: "Additional Info",
          titleClass: 'title',
          body: data
        });
      }
    });
  });
  $('.cancel_query_button').live('click', function() {
    $(this).siblings('img').show();
  });
  load_queries();
});