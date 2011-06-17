$(document).ready ->

  # Function updates the queries table
  update_table = () ->
    table_name = $('#table-select').val()
    $.ajax(
      url: 'runtime/' + table_name
      dataType: 'script'
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






  $(".complete").live('click', ->
    if $(this).is ':checked'
      $('tr').each( ->
        if $(this).attr('r') == 'false' and $(this).attr('i') == 'false'
          $(this).removeAttr("hidden")
      )
    else if $(this).not ':checked'
      $('tr').each( ->
        if $(this).attr('r') == 'false' and $(this).attr('i') == 'false'
          $(this).attr("hidden", true)
        )
  )

  $(".running").live('click', ->
    if $(this).is ':checked'
      $('tr').each( ->
        if $(this).attr('r') == 'true'
          $(this).removeAttr("hidden")
      )
    else if $(this).not ':checked'
      $('tr').each( ->
        if $(this).attr('r') == 'true'
          $(this).attr("hidden", true)
        )
  )

  $(".interrupted").live('click', ->
    if $(this).is ':checked'
      $('tr').each( ->
        if $(this).attr('i') == 'true'
          $(this).removeAttr("hidden")
      )
    else if $(this).not ':checked'
      $('tr').each( ->
        if $(this).attr('i') == 'true'
          $(this).attr("hidden", true)
        )
  )



  $(".both").live('click', ->
    if $(this).is ':checked'
      $('tr').each( ->
        $(this).removeAttr("hidden")
      )
  )

  $(".on").live('click', ->
    if $(this).is ':checked'
      $('tr').each( ->
        if $(this).attr('s') == 'true'
          $(this).removeAttr("hidden")
        else if $(this).attr('s') == 'false'
          $(this).attr("hidden", true)
      )
  )

  $(".off").live('click', ->
    if $(this).is ':checked'
      $('tr').each( ->
        if $(this).attr('s') == 'false'
          $(this).removeAttr("hidden")
        else if $(this).attr('s') == 'true'
          $(this).attr("hidden", true)
      )
  )






  #$('.filter-section').live("click", -> update_table())
