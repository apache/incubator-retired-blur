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
    #to-do: Add the functionality for the jquery dialog and asking to be sure of deleting underlying
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
    })
    
  setup_filter_tree()

  #Listener to delete a table
  $(".delete-table").live('click', ->
    table_name = $(this).attr('table_name')
    $(".ui-confirm").attr("table", table_name)
    $("#confirm-dialog").empty()
    $("#confirm-dialog").append("<p>This will delete the <em>\"" + table_name + "\" </em> table, Do you wish to continue?</p>")
    $(".ui-confirm").dialog({modal: true, draggable: false, resizable: false, title: "Confirm Delete", width: "450px"})
    #load up the dialog
    #delete_table( $(this).attr('table_name') )
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
  $('.ui-widget-overlay').live("click", -> $(".ui-confirm").dialog("close"))
  #listeners for the cancel/OK buttons on the dialog
  $('.cancel').live("click", -> $(".ui-confirm").dialog("close"))
  $('.ok').live("click", -> 
    delete_table($(".ui-confirm").attr("table"), $("#underlying-confirm").is(":checked"))
    $(".ui-confirm").dialog("close"))
