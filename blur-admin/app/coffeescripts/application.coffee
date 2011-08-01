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
    if $('#help-menu').has('div').length == 0
      #ajax request for help
      $.ajax '/help',
        type: 'GET',
        success: (data) ->
          #load the html into the help div and display
          $('#help-menu').html(data)
          #if logic for controllers that are shared by different pages have to select by action
          if $('#help-menu').data().controller == "zookeepers" && $('#help-menu').data().action == "show_current"
            correct_child = $('.help-section#environment').children('.help-content')
          else if $('#help-menu').data().controller == "users" && $('#help-menu').data().action == "index"
            correct_child = $('.help-section#admin').children('.help-content')
          else
            #if it is not the special case select by the controller as the id
            correct_child = $('.help-section#' + $('#help-menu').data().controller).children('.help-content')
          correct_child.toggle()
    #after the help menu is built show the dialog
    $('#help-menu').dialog
      title: "Help Menu",
      height: 'auto',
      width: 710,
      modal: false,
      resizable: false,
      draggable: true,
      position: 'top'
      
  $('.help-section').live 'click', ->
    $(this).children('.help-content').toggle('fast')
