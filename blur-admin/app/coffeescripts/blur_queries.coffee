$(document).ready ->

  add_color = '#95D169' #color to highlight added rows
  update_color = '#E68F00'
  remove_color = '#E01E00'
  
  #grabs the current filter values and shows them in the header
  show_filter_choices = () ->
    info = 'Current Filters: '
    info += $('#created_at_time :selected').text() + " | "
    info += $('#super_query_on :selected').text() + " | "
    info += $('#running :selected').text() + " | "
    info += $('#interrupted :selected').text()

  # adds time to data-age of selected elements, and 
  # retires them if past retirement age
  age_and_retire = (selector, age, retirement_age) ->
    # increment created-ago time
    retired_rows = []
    for row in selector
      current_age = parseFloat($(row).attr('data-age')) + age
      if current_age > retirement_age
        #highlight row and remove
        retired_rows.push row
      else
        $(row).attr 'data-age', "#{current_age}"

    if retired_rows.length isnt 0
      $(retired_rows).effect 'highlight', {color: remove_color}, 'slow', ->
          $(this).remove()

  # set default filter options
  # keeps track of previous filter options
  super_query_filter = ""
  created_at_filter = "1"
  running_filter = ""
  interrupted_filter = ""
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
      #
      # The table must also be replaced if filtering for a
      # transient state, i.e. a non-complete query
      # or a non-interrupted query,
      # because otherwise when the query leaves this state
      # it will not be updated because the results will not
      # include it.

      replace_table = super_query_filter != $('#super_query_on').val() or
                      created_at_filter  != $('#created_at_time').val() or
                      running_filter     != $('#running').val() or
                      interrupted_filter != $('#interrupted').val() or
                      blur_table_id      != $('#blur_table_id').val() or
                      'true'             == $('#running').val() or
                      'false'            == $('#interrupted').val()

      if replace_table
        # reset last filter options
        super_query_filter = $('#super_query_on').val()
        created_at_filter  = $('#created_at_time').val()
        interrupted_filter = $('#interrupted').val()
        running_filter     = $('#running').val()
        blur_table_id      = $('#blur_table_id').val()
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
      if $(this).find('#refresh_period').val() is 'continuous'
        $('#filter_form').submit()
      else
        $(this).find("input[type=submit]").removeAttr('disabled')
        $('#filter_spinner').hide()
    .live 'ajax:success', (evt, data, status, xhr) ->
      rows = $($.trim(data)) # rails renders whitespace if there are no rows

      existing_rows = $("#queries-table > tbody > tr.blur_query")
      if existing_rows.length isnt 0
        # if completely replacing the table, check for stale rows
        if replace_table
          stale_rows = []
          for existing_row in existing_rows
            if rows.filter('#' + $(existing_row).attr('id')).length is 0
              stale_rows.push(existing_row)
          $(stale_rows).effect 'highlight', {color: remove_color}, 'slow', ->
              $(this).remove()

        # if there are existing rows, then check for updates
        updated_rows = $.map rows, (row) ->
          if existing_rows.filter('#' + $(row).attr('id')).length isnt 0
            # update existing row
            existing_rows.filter('#' + $(row).attr('id')).replaceWith(row)
            row
          else
            null
        if updated_rows.length isnt 0
          $(updated_rows).effect 'highlight', {color: update_color}, 'slow'
          new_rows = rows.not updated_rows

      # if not already filtered of updated rows, every row is a new row
      new_rows ?= rows
      new_rows.prependTo($('#queries-table > tbody'))
        .effect 'highlight', {color: add_color}, 'slow'

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

  #when the page loads show the currently selected filters
  $('#current').text(show_filter_choices())
  
  $('#filter_form').live 'change', ->
    $('#current').text(show_filter_choices())
  
  $('#filter_wrapper').accordion
    collapsible: true
    autoHeight: false
    active: false
    change: ->
      $('#current').text(show_filter_choices())
  
  # Listener for the table selector
  $('#blur_table_id').live 'change', ->
    $('#filter_form').submit()

  timer = null
  period = null

  set_timer = () ->
    $('#filter_form').submit()
    timer = setTimeout(set_timer, period)

  # Listener for auto refresh queries
  $('#refresh_period').live 'change', ->
    clearTimeout(timer)
    if $(this).val() is 'continuous'
      $('#filter_form').submit()
      $('#pause').show()
    else if $(this).val() isnt 'false'
      period = $(this).val() * 1000
      set_timer()
    if $(this).val() isnt 'continuous'
      $('#pause').hide()
      $('#pause').removeClass 'ui-icon-play'
      $('#pause').addClass 'ui-icon-pause'

  # Listener for pause/play button
  $('#pause').live 'click', ->
    if $(this).hasClass 'ui-icon-pause'
      $(this).removeClass 'ui-icon-pause'
      $(this).addClass 'ui-icon-play'
    else
      $(this).removeClass 'ui-icon-play'
      $(this).addClass 'ui-icon-pause'

  # Listener for cancel button (launches dialog box)
  $('.cancel_query_button').live 'click', ->
    form = $(this).closest 'form.cancel'
    $("<div class='confirm_enable_disable'>Are you sure?</div>").dialog
      modal: true,
      draggable: false,
      resizable: false,
      title: "Cancel",
      buttons:
        "Yes": ->
          form.submit()
          $(this).dialog 'close'
        "Cancel": ->
          $(this).dialog 'close'
      close: ->
        $(this).remove()
