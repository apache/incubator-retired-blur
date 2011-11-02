$(document).ready ->  
  #grabs the current filter values and shows them in the header
  update_filter_choices = () ->
    filters = "within past #{$('#created_at_time :selected').text()}"
    if $('#super_query_on').val() == 'true'
      filters += " | super query on"
    else if $('#super_query_on').val() == 'false'
      filters += " | super query off"
    if $('#state').val() == "0"
      filters += " | running"
    else if $('#state').val() == "1"
      filters += " | interrupted"
    else if $('#state').val() == "2"
      filters += " | completed"
    if $("#users").val() == "only"
      filters += " | Only users "
    else if $("#users").val() == "exclude"
      filters += " | Exclude users "
    if $("#users").val() != ""
      filters += $('#userid').val()
    $('#filters').html(filters)

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
      $(retired_rows).remove()

  # set default filter options
  # keeps track of previous filter options
  super_query_filter = ""
  created_at_filter = "1"
  state_filter = ""
  user_filter = ""
  username_filter = ""
  blur_table_id = ""
  last_refresh = new Date()
  replace_table = null
  time_since_refresh = null
  
  filter_queries = () ->
    replace_table = super_query_filter != $('#super_query_on').val() or
                    created_at_filter  != $('#created_at_time').val() or
                    state_filter       != $('#state').val() or
                    blur_table_id      != $('#blur_table_id').val() or
                    user_filter        != $('#users').val() or
                    username_filter    != $('#userid').val()
    if replace_table
      # reset last filter options
      super_query_filter = $('#super_query_on').val()
      created_at_filter  = $('#created_at_time').val()
      state_filter       = $('#state').val()
      user_filter        = $('#users').val()
      username_filter    = $('#userid').val()
      blur_table_id      = $('#blur_table_id').val()
      $('#time_since_refresh').val ''
    else
      # set time since last refresh
      now = new Date()
      time_since_refresh = (now - last_refresh) / 1000 
      $('#time_since_refresh').val time_since_refresh
      last_refresh = now
    $('#filter_spinner').show()
    if $('#pause').hasClass 'ui-icon-pause'
      age_and_retire($('tr.blur_query'), time_since_refresh, created_at_filter * 60)
    $.ajax Routes.refresh_path(), {
      type: 'GET'
      data: $('#blur_table_id, #time_since_refresh, #refresh_period, #filter_form select, #filter_form input').serialize()
      complete: () ->
        # update current filters
        update_filter_choices()
        # resubmit if continuous and pause is not pressed***
        if $('#refresh_period').val() is 'continuous' and $('#pause').hasClass 'ui-icon-pause'
          filter_queries()
        else
          $('#filter_spinner').hide()
      success: (data) ->
        rows = $($.trim(data)) # rails renders whitespace if there are no rows

        # Updates rows if pause button is not pressed***
        if $('#pause').hasClass 'ui-icon-pause'
          existing_rows = $("#queries-table > tbody > tr.blur_query")
          if existing_rows.length isnt 0
            # if completely replacing the table, check for stale rows
            if replace_table
              stale_rows = []
              for existing_row in existing_rows
                if rows.filter('#' + $(existing_row).attr('id')).length is 0
                  stale_rows.push(existing_row)
              $(stale_rows).remove()

            # if there are existing rows, then check for updates
            updated_rows = $.map rows, (row) ->
              if existing_rows.filter('#' + $(row).attr('id')).length isnt 0
                # update existing row
                existing_rows.filter('#' + $(row).attr('id')).replaceWith(row)
                row
              else
                null
            if updated_rows.length isnt 0
              #$(updated_rows).effect 'highlight', {color: update_color}, 'slow'
              new_rows = rows.not updated_rows

          # if not already filtered of updated rows, every row is a new row
          new_rows ?= rows
          new_rows.prependTo($('#queries-table > tbody'))
    }
    
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
  # Ajax request handling for more info link
  $('a.times')
    .live 'ajax:success', (evt, data, status, xhr) ->
      $(data).dialog
        modal: true
        draggable: false
        resizable: false
        width: 'auto'
        title: "Query Time"
        close: (event, ui) ->
          $(this).remove()
        open: ->
          $('.ui-widget-overlay').bind 'click', -> 
            $('#more-info-table').dialog('close')

  #when the page loads show the currently selected filters
  update_filter_choices()
  
  # Listener for the table selector
  $('#blur_table_id').live 'change', ->
    filter_queries()

  timer = null
  period = null

  set_timer = () ->
    filter_queries()
    timer = setTimeout(set_timer, period)

  # Listener for auto refresh queries
  $('#refresh_period').live 'change', ->
    clearTimeout(timer)
    if $(this).val() is 'continuous'
      filter_queries()
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
      $('#filter_spinner').hide()
    else
      $(this).removeClass 'ui-icon-play'
      $(this).addClass 'ui-icon-pause'
      if $('#refresh_period').val() is 'continuous' and $('#pause').hasClass 'ui-icon-pause'
        filter_queries()

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

  # set off change event on refresh period to get auto refresh started
  $('#refresh_period').trigger('change')
  
  $('#filter_link').click ->
    $('#filter_form').dialog
      autoOpen: true
      modal: true
      draggable: false
      resizable: false
      width: 'auto'
      title: "Filters"
      buttons:
        "Apply": ->
          filter_queries()
          $(this).dialog 'close'
        "Cancel": ->
          $(this).dialog 'close'
          
  $('#users').change ()->
    if $('#users').val() == ''
      $('#user_filters').hide()
    else
      $('#user_filters').show()
      
  $('#unknown_user').click ()->
    user_field = $('#userid')
    user_field.val(user_field.val().replace('unknown', '').trim())
    
    if $(this).is(':checked') && user_field.val() == ''
      user_field.val('unknown')
    else if $(this).is(':checked')
      user_field.val(user_field.val() + " unknown")
    true