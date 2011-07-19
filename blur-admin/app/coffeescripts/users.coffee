$(document).ready ->
  #method to persist the preference to the DB
  save_pref = () ->    
    $.ajax '/preference/save/',
      type: 'POST',
      data: $('#my-cols').sortable('serialize'),
      success: (data) ->
    
  #make this sortable
  $('#my-cols').sortable
    connectWith: "#actual-trash",
    #when the order is changed
    stop: ->
      save_pref()
  
  $('#actual-trash').sortable
    items: "div:not(#trash)"
    receive: (event, ui)->
      $(ui.item).remove()
      $('#' + $(ui.item).attr('id')).removeClass('my-select')
    
  #click listener for the lists of column names
  $('.fam').live 'click', -> 
    $(this).toggleClass('my-select')
    clicked = $('#' + $(this).attr('id') + '.sel-fam')
    #if the element isnt in the list already
    if clicked.length == 0
      $('#no-saved').hide()
      app = $(this).clone().removeClass('fam my-select').addClass('sel-fam')
      $('#my-cols').append app.hide()
      app.fadeIn 'slow', -> save_pref()
    #if the element is already in the list
    else
      clicked.fadeOut 'slow', -> 
        clicked.remove()
        if $('#my-cols').children().length == 1
          $('#no-saved').fadeIn('fast')
        save_pref()