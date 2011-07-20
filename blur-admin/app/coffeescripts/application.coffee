$(document).ready ->
  # Zookeeper context switch
  $('#zookeeper_id').live 'change', ->
    #reload page with new zookeeper
    $(this).closest('form').submit()

  # Remove blue oval around clicked jstree elements
  $('.jstree-clicked').live 'click', ->
    $('.jstree-clicked').removeAttr('class', 'jstree-clicked')

  # Listener to hide dialog on click
  $('.ui-widget-overlay').live "click", -> $(".ui-dialog-content").dialog "close"

  $('[title]').tooltip
    show:
      delay: 250
  $('html').live 'ajax:success', ->
    console.log "success"
    $('[title]').tooltip
      show:
        delay: 250

  $('html').live 'ajax:complete', ->
    console.log "complete"
