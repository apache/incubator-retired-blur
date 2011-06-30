$(document).ready ->
  current_zookeeper = $('#zookeeper_id').val()

  $('#zookeeper_id').live 'change', ->
    #reload page with new zookeeper
    console.log $(this).val()
    $(this).closest('form').submit()

  
  $('#change_current_zookeeper')
    .live 'ajax:beforeSend', (evt, xhr, settings) ->
      $('#filter_spinner').show()
    .live 'ajax:complete', (evt, xhr, status) ->
      $('#filter_spinner').hide()
    .live 'ajax:success', (evt, data, status, xhr) ->
      $(".zookeeper##{current_zookeeper}").replaceWith(data)
      current_zookeeper = $('#zookeeper_id').val()
    .live 'ajax:error', (evt, xhr, status, error) ->
      # TODO: Add error handling
