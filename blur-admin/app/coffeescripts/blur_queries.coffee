$(document).ready ->
  # set default filter options
  # keeps track of previous filter options
  super_query_filter = ""
  created_at_filter = "1"
  blur_table_id = ""
  last_refresh = new Date()
  replace_table = null
  time_since_refresh = null

  # adds time to data-age of selected elements, and 
  # retires them if past retirment age
  age_and_retire = (selector, age, retirement_age) ->
    # increment created-ago time
    for row in selector
      current_age = parseFloat($(row).attr('data-age')) + age
      if current_age > retirement_age
        #highlight row and remove
        $(row).effect 'highlight', {color: 'red'}, 'slow', () ->
            $(this).remove()
      else
        $(row).attr 'data-age', "#{current_age}"

  # function to remove an index from an array
  Array::remove = (e) -> @[t..t] = [] if (t = @.indexOf(e)) > -1

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
    .live 'ajax:complete', (evt, xhr, status) ->
      $(this).find("input[type=submit]").removeAttr('disabled')
      $('#filter_spinner').hide()
    .live 'ajax:success', (evt, data, status, xhr) ->
      # replace white space in html because rails is stupid enough to put white space
      # between and around rendered collections
      rows = $ data.replace(/<\/tr>\s+<tr/g, '</tr><tr').replace(/(^\s*)|(\s*$)/g, '')

      if replace_table
        $('#queries-table > tbody').empty() #delete all rows
        $('#queries-table > tbody').append(rows) #insert new rows
      else
        # remove rows older than the filter time
        age_and_retire($('tr.blur_query'), time_since_refresh, created_at_filter * 60)

        # replace duplicate rows
        for row in rows
          #check if a row with the same id exists in the table already, if so replace it and
          #remove the row from the data object
          id = $(row).attr 'id'
          if $("#queries-table > tbody > ##{id}").replaceWith(row) isnt []
            console.log "Existing Row: Updating..."
            $("#queries-table > tbody > ##{id}").effect 'highlight', {color: 'blue'}, 'slow'
            row = []

        # prepend remaining rows to table
        $("#queries-table > tbody").prepend(rows)
        rows.effect 'highlight', {color: 'green'}, 'slow'
        console.log rows

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
