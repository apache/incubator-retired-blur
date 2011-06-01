$(document).ready ->
  #Creates an AJAX request to enable or disable a table upon
  #Check or uncheck of 'Enable / Disable' checkbox
  $(".enable").click( ->
    #Determine whether table is to be enabled or disabled
    if $(this).is ':checked'
      action = "enable"
    else if $(this).not ':checked'
      action = "disable"

    url = "/data/table/" + $(this).attr('id') + "/" + action
    
    result = $.ajax(
      url: url
      type: 'PUT' )
  )

  #FUNCTION delete a table
  $(".delete-table").click( ->
    confirmation = confirm("Are you sure you want to delete this table?")
    if confirmation
      url = "/data/table/" + $(this).attr('id')
      result = $.ajax(
        url: url
        type: 'DELETE')
  )

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
