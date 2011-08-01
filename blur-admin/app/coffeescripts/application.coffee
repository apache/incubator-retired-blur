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
  
  #faster tooltip for the search table
  $('[title]:not(.action-icon)').tooltip
    show:
      delay: 250
  
  #slower tooltip for the saved buttons  
  $('.action-icon').tooltip
    show:
      delay: 1000
          
  $('html').live 'ajax:success', ->
    # remove old tooltips
    $('.ui-tooltip').remove()
    
    #faster tooltip for the search table
    $('[title]:not(.action-icon)').tooltip
      show:
        delay: 250
        
    #slower tooltip for the saved buttons    
    $('.action-icon').tooltip
      show:
        delay: 1000
        
  #fade out flash messages for logging in and out
  $("#flash").delay(5000).fadeOut("slow")
  
  # Initialize Help
  $('#page-help').click ()->
    #ajax request
    #load dialog
    $('#help-menu').dialog
      title: "Help Menu",
      height: 'auto',
      width: 710,
      modal: false,
      resizable: false,
      draggable: true
      
  $('.help-section').live 'click', ->
    $(this).children('.help-content').toggle('fast')
