$(document).ready ->

  # Function to initialize the filter tree
  setup_filter_tree = () ->
    $('.blur_table_definition').jstree({
      plugins: ["themes", "html_data", "sort", "ui"],
      themes: {
        theme: 'apple',
        icons: false,
      }
    }).bind("select_node.jstree", (event, data) -> 
      $(this).jstree('toggle_node')
    )

  # Ajax request handling for delete
  $('form.delete')
    .live('ajax:beforeSend', (evt, xhr, settings) ->
      $(this).find('input[type=submit]').attr('disabled', 'disabled')
    ).live('ajax:complete', (evt, xhr, status) ->
      $(this).find('input[type=submit]').removeAttr('disabled')
    ).live('ajax:success', (evt, data, status, xhr) ->
      id = $(this).closest('tr').attr('id')
      selector = "table#blur_tables_table > tbody > tr##{id}"
      $(selector).remove()
    ).live('ajax:error', (evt, xhr, status, error) ->
      console.log "Error in delete ajax call"
    )

  # Ajax request handling for enable/disable
  $('form.update')
    .live('ajax:beforeSend', (evt, xhr, settings) ->
      $(this).find('input[type=submit]').attr('disabled', 'disabled')
    ).live('ajax:complete', (evt, xhr, status) ->
      $(this).find('input[type=submit]').removeAttr('disabled')
    ).live('ajax:success', (evt, data, status, xhr) ->
      row = $(this).closest('tr')
      row.siblings("##{row.attr('id')}").remove()
      row.replaceWith data
      setup_filter_tree()
    ).live('ajax:error', (evt, xhr, status, error) ->
      console.log "Error in update ajax call"
    )

  # Calls the function to initialize the filter tree
  setup_filter_tree()

  # Listener to hide dialog on click
  $('.ui-widget-overlay').live("click", -> $(".ui-dialog-content").dialog("close"))

  # Listeners for the cancel/OK buttons on the dialog
  $('.cancel').live("click", -> $(".ui-dialog-content").dialog("close"))
  $('.ok').live("click", -> 
    delete_table($(".ui-confirm").attr("table"), $("#underlying-confirm").is(":checked"))
    $(".ui-dialog-content").dialog("close"))
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
