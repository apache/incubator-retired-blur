$(document).ready ->

  # Function updates the queries table
  update_table = (table_name) ->
    $.ajax(
      url: 'runtime/' + table_name
      dataType: 'script'
    )

  # Function cancels a query
  update_query = (table_name, uuid, cancel) ->
    url = '/runtime/' + table_name + '/' + uuid
    data = 'cancel=' + cancel
    $.ajax(
      data: data
      url: url
      type: 'PUT'
    )
  
  # Function queries for the new info
  show_info = (uuid) ->
    $.ajax(
      url: '/runtime/queries/' + uuid
      type: 'GET'
    )

  #change listener for the table selector
  $('#table-select').live('change', ->
    update_table($(this).val())
  )
  
  #sets up the listeners for the cancel buttons 
  $('.cancel').live('click', ->
    uuid = $(this).attr('uuid')
    table_name = $(this).attr('table_name')
    cancel = $(this).attr('cancel')
    update_query(table_name, uuid, cancel)
    )
    
  $('.info').live('click', ->
    #to-do call the show dialog method afterwards
    $('#more-info-container').load('runtime/queries/' + $(this).attr('id'), alert "hello")
  )

  $('[title]').tooltip({});