$(document).ready ->

  # Function to initialize a filter tree on the passed in element
  setup_filter_tree = (selector) ->
    selector.jstree
      plugins: ["themes", "html_data", "sort", "ui"],
      themes:
        theme: 'apple',
        icons: false,
    .bind "select_node.jstree", (event, data) -> 
      $(this).jstree('toggle_node')

  # Calls the function to initialize the filter tree
  setup_filter_tree($('.blur_table_definition'))

  # Ajax request handling for enable/disable
  $('form.update')
    .live 'ajax:beforeSend', (evt, xhr, settings) ->
      $(this).find('input[type=submit]').attr('disabled', 'disabled')

    .live 'ajax:complete', (evt, xhr, status) ->
      $(this).find('input[type=submit]').removeAttr('disabled')
    .live 'ajax:success', (evt, data, status, xhr) ->
      row = $(this).closest('tr')
      row.siblings("##{row.attr('id')}").remove()
      row.replaceWith data
      setup_filter_tree($('.blur_table_definition'))
    .live 'ajax:error', (evt, xhr, status, error) ->
      console.log "Error in update ajax call"
    
  # Ajax request handling for schema
  $('a.schema')
    .live 'ajax:success', (evt, data, status, xhr) ->
      console.log data
      $(data).dialog
        modal: true
        draggable: false
        resizable: false
        title: "Hosts and Shards"
        close: (event, ui) ->
          $(this).remove()
      $('ui-widget-overlay').bind 'click', ->
        $('.schema').dialog 'close'
      setup_filter_tree($('.schema_tree'))

  # Listener for delete button (launches dialog box)
  $('.delete_blur_table_button').live 'click', ->
    form = $(this).closest('form.delete')
    $("<div class='confirm_delete'>Do you want to delete the underlying table index?</div>").dialog
      buttons: {
        "Delete Index": ->
          form.find('#delete_index').val('true')
          form.submit()
          $(this).dialog('close')
        "Preserve Index": ->
          form.submit()
          $(this).dialog('close')
        "Cancel": ->
          $(this).dialog('close')
      }
      close: ->
        $(this).remove()

  # Ajax request handling for delete
  $('form.delete')
    .live 'ajax:beforeSend', (evt, xhr, settings) ->
      $(this).find('.delete_blur_table_button').attr('disabled', 'disabled')
    .live 'ajax:complete', (evt, xhr, status) ->
      $(this).find('.delete_blur_table_button').removeAttr('disabled')
    .live 'ajax:success', (evt, data, status, xhr) ->
      id = $(this).closest('tr').attr('id')
      selector = "table#blur_tables_table > tbody > tr##{id}"
      $(selector).remove()
    .live 'ajax:error', (evt, xhr, status, error) ->
      console.log "Error in delete ajax call"

  # Listener to hide dialog on click
  $('.ui-widget-overlay').live("click", -> $(".ui-dialog-content").dialog("close"))

  # Listeners for the cancel/OK buttons on the dialog
  $('.cancel').live "click", -> $(".ui-dialog-content").dialog("close")
  $('.ok').live "click", -> 
    delete_table($(".ui-confirm").attr("table"), $("#underlying-confirm").is(":checked"))
    $(".ui-dialog-content").dialog("close")
  $('#delete-label').live "click", -> 
    if $("#underlying-confirm").is(":checked")
      $("#underlying-confirm").attr("checked", false)
    else
      $("#underlying-confirm").attr("checked", true)

  # Remove blue oval around clicked jstree elements
  $('.jstree-clicked').live 'click', ->
    $('.jstree-clicked').removeAttr('class', 'jstree-clicked')
