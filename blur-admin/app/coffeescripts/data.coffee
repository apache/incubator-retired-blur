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
  delete_table = (table_name) ->
    confirmation = confirm("Are you sure you want to delete #{table_name}?")
    if confirmation
      url = "/data/" + table_name
      result = $.ajax(
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
    delete_table( $(this).attr('table_name') )
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
