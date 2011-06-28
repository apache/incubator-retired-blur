$(document).ready ->
  current_blur_zookeeper_instance = $('#zookeeper_instance_id').val()

  $('#zookeeper_instance_id').live 'change', ->
    #reload page with new zookeeper instance
    console.log $(this).val()
    $(this).closest('form').submit()

  
  $('#change_current_zookeeper_instance')
    .live 'ajax:beforeSend', (evt, xhr, settings) ->
      $('#filter_spinner').show()
    .live 'ajax:complete', (evt, xhr, status) ->
      $('#filter_spinner').hide()
    .live 'ajax:success', (evt, data, status, xhr) ->
      $(".blur_zookeeper_instance##{current_blur_zookeeper_instance}").replaceWith(data)
      current_blur_zookeeper_instance = $('#zookeeper_instance_id').val()
    .live 'ajax:error', (evt, xhr, status, error) ->
      # TODO: Add error handling
