$(document).ready ->
  #visible_column_count = $('#queries-table thead th').length
  # Load queries into table
  load_queries = () ->
    $.ajax Routes.refresh_path(), {
      type: 'GET'
      #data: $('#blur_table_id, #time_since_refresh, #refresh_period, #filter_form select, #filter_form input').serialize()
      success: (data) ->
        # Update rows
        if data.length == 0
          clear_table()
          add_row "<tr id='no_queries'><td colspan='#{visible_column_count}'>No Available Queries</td></tr>"
        else
          $.each data, () ->
            row_data = this
            cleaned_row_data = $.map row_data[2..row_data.length], (item, idx) ->
              if idx == 1 && row_data.length == 8
                item = truncate(item)
              print_value(item)
            cleaned_row_data[cleaned_row_data.length] = build_actions row_data
            add_row cleaned_row_data
    }
  clear_table = () ->
    $('#queries-table tbody tr').remove()
  add_row = (data) ->
    if $.isArray(data)
      row = "<tr>"
      $.each data, ->
        row = "#{row}<td>#{this}</td>"
      row = row + "</tr>"
      $('#queries-table tbody').prepend(row)
    else
      $('#queries-table tbody').prepend(data)
  print_value = (conditional, block, default_message = "Not Available") ->
    return default_message unless conditional
    return conditional unless block
    block(conditional)
  truncate = (value, length, ommission) ->
    return null unless value
    return value unless value.length > length
    "#{value.substring(0,length)}#{ommission ? ommission : ''}"
  build_actions = (row_data) ->
    action_content = ''
    action_content = "<button class='.cancel_query_button btn' data-query_id='#{row_data[0]}'>Cancel</button>" if row_data[1] && row_data[row_data.length-1].indexOf('Complete') == -1 && row_data[row_data.length-1].indexOf('Interrupted') == -1
    "#{action_content} <a href='javascript:void(0)' class='more_info' data-query_id='#{row_data[0]}'>More Info</a> | <a href='javascript:void(0)' class='times' data-query_id='#{row_data[0]}'>Times</a>"
    
#    = link_to "More Info", more_info_blur_query_path(blur_query), :class => 'more_info', :remote => true, :style => 'margin-right: 3px'
#    = link_to "Times", times_blur_query_path(blur_query), :class => 'times', :remote => true

  # Initialize page
  load_queries()