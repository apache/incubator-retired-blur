$(document).ready ->

  add_color = '#95D169' #color to highlight added rows
  update_color = '#E68F00'
  remove_color = '#E01E00'

  # checks if the empty row is needed/not needed and inserts / removes
  # it accordingly
  empty_row = ->
    rows = $('#queries-table > tbody').children()
    if rows.length is 0
      $('#queries-table > tbody').prepend(
        '''
<tr id='no-queries-row'>
  <td colspan='9'>
    <b>No Available Queries</b>
  </td>
</tr>
        '''
      )
    else if $('#queries-table > tbody').children().length > 1 and
            $('#queries-table > tbody > #no-queries-row').length is 1
      $('#no-queries-row').remove()

  # adds time to data-age of selected elements, and 
  # retires them if past retirment age
  age_and_retire = (selector, age, retirement_age) ->
    # increment created-ago time
    for row in selector
      current_age = parseFloat($(row).attr('data-age')) + age
      if current_age > retirement_age
        #highlight row and remove
        $(row).effect 'highlight', {color: remove_color}, 'slow', ->
            $(this).remove()
            empty_row()
      else
        $(row).attr 'data-age', "#{current_age}"


  # set default filter options
  # keeps track of previous filter options
  super_query_filter = ""
  created_at_filter = "1"
  blur_table_id = ""
  last_refresh = new Date()
  replace_table = null
  time_since_refresh = null

  # Ajax request handling for filter form
  $('#filter_form')
    .live 'ajax:before', ->
      # Modify form to include time since last refresh,
      # but only if the filter params have not changed

      # determine if params have changed, if so, this
      # means that the request should completely replace
      # the table with new rows, and not include a 
      # time_since_refresh

      replace_table = super_query_filter != $('#super_query_on').val() or
                      created_at_filter != $('#created_at_time').val() or
                      blur_table_id != $('#blur_table_id').val()
      if replace_table
        # reset last filter options
        super_query_filter = $('#super_query_on').val()
        created_at_filter = $('#created_at_time').val()
        blur_table_id = $('#blur_table_id').val()
        $('#time_since_refresh').val ''
      else
        # set time since last refresh
        now = new Date()
        time_since_refresh = (now - last_refresh) / 1000 
        $('#time_since_refresh').val time_since_refresh
        last_refresh = now
    .live 'ajax:beforeSend', (evt, xhr, settings) ->
      $(this).find("input[type=submit]").attr('disabled', 'disabled')
      $('#filter_spinner').show()
      # remove rows older than the filter time
      age_and_retire($('tr.blur_query'), time_since_refresh, created_at_filter * 60)
    .live 'ajax:complete', (evt, xhr, status) ->
      $(this).find("input[type=submit]").removeAttr('disabled')
      $('#filter_spinner').hide()
      empty_row()
    .live 'ajax:success', (evt, data, status, xhr) ->
      rows = $($.trim(data)) # rails renders whitespace if there are no rows

      if replace_table
        $('#queries-table > tbody').empty() #delete all rows
        $('#queries-table > tbody').append(rows) #insert new rows
      else

        for row in $.makeArray(rows).reverse()
          #check if a row with the same id exists in the table already, if so replace it and
          #remove the row from the data object
          id = $(row).attr 'id'
          if $("#queries-table > tbody > ##{id}").length is 0
            # add new row
            $("#queries-table > tbody").prepend(row)
            $(row).effect 'highlight', {color: add_color}, 'slow'
          else
            # replace existing row
            $("#queries-table > tbody > ##{id}").replaceWith(row)
            $("#queries-table > tbody > ##{id}").effect 'highlight', {color: update_color}, 'slow'

      $('[title]').tooltip()
    .live 'ajax:error', (evt, xhr, status, error) ->
      # TODO: Add error handling

  # Ajax request handling for cancel form
  $('form.cancel')
    .live 'ajax:beforeSend', (evt, xhr, settings) ->
      $(this).find('input[type=submit]').attr('disabled', 'disabled')
    .live 'ajax:complete', (evt, xhr, status) ->
      $(this).find('input[type=submit]').removeAttr('disabled')
    .live 'ajax:success', (evt, data, status, xhr) ->
      $(this).closest('tr').replaceWith(data)
    .live 'ajax:error', (evt, xhr, status, error) ->
      # TODO: Add error handling
      console.log "error"

  # Ajax request handling for more info link
  $('a.more_info')
    .live 'ajax:success', (evt, data, status, xhr) ->
      $(data).dialog
        modal: true
        draggable: false
        resizable: false
        width: 'auto'
        title: "Additional Info"
        close: (event, ui) ->
          $(this).remove()
        open: ->
          $('.ui-widget-overlay').bind 'click', -> 
            $('#more-info-table').dialog('close')

  $('#filter_wrapper').accordion
    collapsible: true
    autoHeight: false
    active: false
  
  # Displays the full query string on hover
  $('[title]').tooltip()

  # Listener for the table selector
  $('#blur_table_id').live 'change', ->
    $('#filter_form').submit()

  timer = null
  period = null

  set_timer = () ->
    console.log 'refreshing...'
    $('#filter_form').submit()
    timer = setTimeout(set_timer, period)

  # Listener for auto refresh queries
  $('#refresh_period').live 'change', ->
    clearTimeout(timer)
    unless $(this).val() is '-1'
      period = $(this).val() * 1000
      set_timer()
