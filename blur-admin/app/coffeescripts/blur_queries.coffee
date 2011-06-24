$(document).ready ->

  # Ajax request handling for filter form
  $('#filter_form')
    .live 'ajax:beforeSend', (evt, xhr, settings) ->
      $(this).find("input[type=submit]").attr('disabled', 'disabled')
      $('#filter_spinner').show()
    .live 'ajax:complete', (evt, xhr, status) ->
      $(this).find("input[type=submit]").removeAttr('disabled')
      $('#filter_spinner').hide()
    .live 'ajax:success', (evt, data, status, xhr) ->
      $('#queries-table').replaceWith(data)
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
