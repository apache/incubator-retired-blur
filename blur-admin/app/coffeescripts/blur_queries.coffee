$.extend $.fn.dataTableExt.oStdClasses, {
  "sSortAsc":"header headerSortDown",
  "sSortDesc":"header headerSortUp",
  "sSortable":"header"
}
  
$(document).ready ->
  visible_column_count = $('#queries-table thead th').length
  refresh_rate = -1
  refresh_timeout = null
  data_table = null  
  # Load queries into table
  load_queries = () ->  
    data_table = $('#queries-table').dataTable({
      "sDom":"<'row'<'span8'i><'span1'r><'span8'f>>t",
      bPaginate: false,
      bProcessing: true,
      bDeferRender: true,
      "oLanguage": {
        "sInfoEmpty": "",
        "sInfo": "Displaying _TOTAL_ queries",
        "sSearch": "Filter queries:",
        "sZeroRecords": "No queries to display",
        "sInfoFiltered": "(filtered from _MAX_ total queries)"
      },
      sAjaxSource: Routes.refresh_path(1),
      aoColumns: table_cols(),
      fnRowCallback: process_row,
    });
    add_refresh_rates(data_table)
    $('#queries-table').ajaxComplete (e, xhr, settings) ->
      if settings.url.indexOf('/blur_queries/refresh') >= 0
        if refresh_rate > -1
          refresh_timeout = setTimeout ->
            range_time_limit = $('.time_range').find('option:selected').val()
            data_table.fnReloadAjax Routes.refresh_path(range_time_limit)
          , refresh_rate * 1000
    $('.time_range').live 'change', ->
      range_time_limit = $(@).find('option:selected').val()
      data_table.fnReloadAjax Routes.refresh_path(range_time_limit)
  table_cols = () ->
    return [{"mDataProp":"userid"},{"mDataProp":"query", "sWidth": "500px"},{"mDataProp":"tablename"},{"mDataProp":"start"},{"mDataProp":"time"},{"mDataProp":"status", "sWidth": "200px"},{"mDataProp":"state", "bVisible":false},{"mDataProp":"action"}] if visible_column_count == 8
    [{"mDataProp":"userid"},{"mDataProp":"tablename"},{"mDataProp":"start"},{"mDataProp":"time"},{"mDataProp":"status", "sWidth": "150px"},{"mDataProp":"state", "bVisible":false},{"mDataProp":"action"}]
  process_row = (row, data, rowIdx, dataIdx) ->
    action_td = $('td:last-child', row)
    if action_td.html() == ''
      action_td.append("<a href='#{Routes.more_info_blur_query_path(data['id'])}' class='more_info' data-remote='true' style='margin-right: 3px'>More Info</a>")
      if data['state'] == 0 && data['can_update']
        action_td.append("<form accept-charset='UTF-8' action='#{Routes.blur_query_path(data['id'])}' class='cancel' data-remote='true' method='post'><div style='margin:0;padding:0;display:inline'><input name='_method' type='hidden' value='put'></div><input id='cancel' name='cancel' type='hidden' value='true'><input class='cancel_query_button btn' type='submit' value='Cancel'></form>")
    time = data.time.substring(0, data.time.indexOf(' ')).split(':')
    timeModifier = data.time.substring(data.time.indexOf(' ') + 1) == 'PM'
    timeInSecs = (if timeModifier then (parseInt(time[0]) + 12) else parseInt(time[0])) * 3600 + parseInt(time[1]) * 60 + parseInt(time[2])
    dateNow = new Date()
    timeNowSecs = dateNow.getHours() * 3600 + dateNow.getMinutes() * 60 + dateNow.getSeconds()
    if data.state == 'Running' && Math.abs(timeNowSecs - timeInSecs) > 3600
      $(row).addClass('oldRunning')
    row
  add_refresh_rates = (data_table) ->
    refresh_content = '<div class="span4">Auto Refresh: '
    options = [{'key':'Off', 'value':-1},{'key':'10s', 'value':10},{'key':'1m', 'value':60},{'key':'10m', 'value':600}]
    
    $.each options, (idx, val) ->
      link_class = if idx == 0 then 'selected' else 'unselected'
      refresh_content += "<a href='javascript:void(0)' class='refresh_option #{link_class}' data-refresh_val='#{val.value}'>#{val.key}</a>"
    
    refresh_content += '</div>'
    $('#queries-table_wrapper > .row:first-child').prepend(refresh_content)
    $('a.refresh_option').click () ->
      $('a.refresh_option').removeClass('selected').addClass('unselected')
      $(this).addClass('selected').removeClass('unselected')
      prev_refresh_rate = refresh_rate
      refresh_rate = $(this).data('refresh_val')
      if prev_refresh_rate == -1
        data_table.fnReloadAjax()
      else if refresh_rate == -1 && refresh_timeout
        clearTimeout(refresh_timeout)
  truncate = (value, length, ommission) ->
    return null unless value
    return value unless value.length > length
    "#{value.substring(0,length)}#{ommission ? ommission : ''}"
    
  $('.more_info').live 'ajax:success', (evt, data, status, xhr) ->
    $().popup
      title: "Additional Info"
      titleClass:'title'
      body:data

  # Initialize page
  load_queries()