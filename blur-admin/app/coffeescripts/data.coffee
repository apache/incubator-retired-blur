$(document).ready ->

  update_table = (table_name, enabled) ->
    url = "/data/" + table_name 
    data = 'enabled=' + enabled
    result = $.ajax(
      data: data
      url: url
      type: 'PUT'
    )

  # Function to delete a table
  delete_table = (table_name, underlying) ->
    url = "/data/" + table_name
    data = 'underlying=' + underlying
    result = $.ajax(
      success: $("tr#" + table_name).remove()
      data: data
      url: url
      type: 'DELETE')


  # Function to initialize the filter tree
  setup_filter_tree = () ->
    $('.table-def').jstree({
      plugins: ["themes", "html_data", "sort", "ui"],
      themes: {
        theme: 'apple',
        icons: false,
      }
    }).bind("select_node.jstree", (event, data) -> 
      $(this).jstree('toggle_node')
    )
    
  setup_filter_tree()

  #Listener to delete a table
  $(".delete-table").live('click', ->
    table_name = $(this).attr('table_name')
    $(".ui-confirm").attr("table", table_name)
    $("#confirm-dialog").empty()
    $("#confirm-dialog").append("<p>This will delete the <em>\"" + table_name + "\" </em> table, Do you wish to continue?</p>")
    $(".ui-confirm").dialog({modal: true, draggable: false, resizable: false, title: "Confirm Delete", width: "450px"})
  )
  
  #Listener to Enable/Disable a table
  $(".enable").live('click', ->
    if $(this).is ':checked'
      enabled = true
    else if $(this).not ':checked'
      enabled = false
    table_name = $(this).attr('table_name')
    update_table(table_name, enabled)
  )
  
  #listener to hide dialog on click
  $('.ui-widget-overlay').live("click", -> $(".ui-dialog-content").dialog("close"))
  #listeners for the cancel/OK buttons on the dialog
  $('.cancel').live("click", -> $(".ui-confirm").dialog("close"))
  $('.ok').live("click", -> 
    delete_table($(".ui-confirm").attr("table"), $("#underlying-confirm").is(":checked"))
    $(".ui-confirm").dialog("close"))
  $('#delete-label').live("click", -> 
    if $("#underlying-confirm").is(":checked")
      $("#underlying-confirm").attr("checked", false)
    else
      $("#underlying-confirm").attr("checked", true)
  )
  
  $('.jstree-clicked').live('click', ->
    $('.jstree-clicked').removeAttr('class', 'jstree-clicked')
  )
  
  $('.host-shards').live('click', ->
    table = $(this).attr('id')
    $('#display.shard-info.' + table ).dialog({modal: true, draggable: false, resizable: false, title: "Shard Server Information", width: "450px"})
  )