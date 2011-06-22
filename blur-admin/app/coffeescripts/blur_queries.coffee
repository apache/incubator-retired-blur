$(document).ready ->

  # Ajax request handling for filter form
  $('#filter_form')
    .live('ajax:beforeSend', (evt, xhr, settings) ->
      $submitButton = $(this).find("input[type=submit]")
      $submitButton.attr('disabled', 'disabled')
      $('#filter_spinner').show()
    ).live('ajax:complete', (evt, xhr, status) ->
      $submitButton = $(this).find("input[type=submit]")
      $submitButton.removeAttr('disabled')
      $('#filter_spinner').hide()
    ).live('ajax:success', (evt, data, status, xhr) ->
    ).live('ajax:error', (evt, xhr, status, error) ->
    )

  # Ajax request handling for more info forms
  $('#filter_form')
    .live('ajax:beforeSend', (evt, xhr, settings) ->
    ).live('ajax:complete', (evt, xhr, status) ->
    ).live('ajax:success', (evt, data, status, xhr) ->
    ).live('ajax:error', (evt, xhr, status, error) ->
    )


  $('#filter_wrapper').accordion ({ 
    collapsible: true
    autoHeight: false
    active: false
  })
  
  # Displays the full query string on hover
  $('[title]').tooltip({})

  # Listener for the table selector
  $('#blur_table_id').live('change', ->
   $('#filter_form').submit() 
  )
