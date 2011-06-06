$(document).ready ->

  # Updates query table with ajax
  update_table = (table_name) ->
    $.ajax(
      url: 'runtime/show/' + table_name
      dataType: 'script'
    )

  #change listener for the table selector
  $('#table-select').change ->
    update_table($('#table-select :selected').val())

  #sets up the listeners for the cancel buttons (mysql)
  $('.cancel').click(() ->
    uuid = $(this).attr('uuid')
    table_name = $(this).attr('table_name')
    cancel = $(this).attr('cancel')

    url = '/runtime/update/' + table_name + '/' + uuid
    $.ajax(
      url: url
      data: 'cancel=' + cancel
      type: 'PUT'
      )
    )
