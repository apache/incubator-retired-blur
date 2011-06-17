$(document).ready ->

  # Function updates the queries table
  update_table = () ->
    table_name = $('#table-select').val()
    time_string = $('#t').val().split(' ')
    if time_string[1] == 'hour'
      time_number = '60'
    else
      time_number = time_string[0]
    $.ajax(
      url: 'runtime/' + table_name + '/' + time_number
      dataType: 'script'
      success: -> filter_queries()
    )


  # Function cancels a query
  update_query = (table_name, uuid, cancel) ->
    url = '/runtime/' + table_name + '/' + uuid
    data = 'cancel=' + cancel
    $("#failed-info").attr("uuid", uuid)
    $("#failed-info").attr("table", table_name)
    $.ajax(
      success: ->
        #$("#failure-message").dialog({modal: true, draggable: false, resizable: false, title: "Cancel Failed", width: "450px"})
        $('.status[id="' + uuid + '"]').html("Interrupted")
        $('input[uuid="' + uuid + '"]').remove()
      failure: ->
        $("#failure-message").dialog({modal: true, draggable: false, resizable: false, title: "Cancel Failed", width: "450px"})
        $('input[uuid="' + uuid + '"]').attr("enabled", true)
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
    update_table()
  )
  
  #sets up the listeners for the cancel buttons 
  $('.cancel').live('click', ->
    $(this).attr("enabled", false)
    uuid = $(this).attr('uuid')
    table_name = $(this).attr('table_name')
    cancel = $(this).attr('cancel')
    update_query(table_name, uuid, cancel)
    )
    
  $('.info').live('click', ->
    $('#more-info-container').load('runtime/queries/' + $(this).attr('id'), -> 
      $("#more-info-container").dialog({modal: true, draggable: false, resizable: false, title: "Additional Info", width: "50%", position: "top"})
      $("#more-info-table").removeAttr("hidden")
    )
  )
  
  #dialog listeners
  $('.cancel').live("click", -> $(".ui-dialog-content").dialog("close"))
  $('.resubmit').live("click", -> 
    update_query($("#failed-info").attr("table"), $("#failed-info").attr("uuid"), true)
  )
  $('.ui-widget-overlay').live("click", -> $(".ui-dialog-content").dialog("close"))

  $('[title]').tooltip({});

  #status filter
  filter_status = () ->
    if $(".complete").is ':checked'
      $('tr').each( ->
        if $(this).attr('r') == 'false' and $(this).attr('i') == 'false'
          $(this).removeAttr("hidden")
      )
    if $(".running").is ':checked'
      $('tr').each( ->
        if $(this).attr('r') == 'true'
          $(this).removeAttr("hidden")
      )
    if $(".interrupted").is ':checked'
      $('tr').each( ->
        if $(this).attr('i') == 'true'
          $(this).removeAttr("hidden")
      )

  #super query filter
  filter_super = () ->
    selected = $("input[@name='super']:checked").val()
    if selected == 'on'
      $('tr').each( ->
        if $(this).attr('s') != 'true'
          $(this).attr("hidden", true)
      )
    else if selected == 'off'
      $('tr').each( ->
        if $(this).attr('s') != 'false'
          $(this).attr("hidden", true)
      )

  filter_queries = () ->
    $('tr').each( -> $(this).attr("hidden", true))
    filter_status()
    filter_super()
    $('.header').removeAttr("hidden")
    #if $('#queries-table tbody').children().length <= 1
      #$('#queries-table tbody').append('<tr><td colspan="8", class="error", bgcolor="#eee">No Available Queries</td></tr>')

  $(".filter-section").live('click', ->
    filter_queries()
  )

  $('#t').live('change', ->
    update_table()
  )

  $('#filters-header').live('click', ->
    if $('#filters-body').attr("hidden")
      $('#filters-body').removeAttr("hidden")
    else
      $('#filters-body').attr("hidden", true)
  )