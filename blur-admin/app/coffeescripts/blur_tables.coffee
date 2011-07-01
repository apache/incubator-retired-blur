$(document).ready ->
  # Close all accordions on page load
  for table in $('div.blur_table')
    $(table).hide()

  # Accordion open/close Listener
  # Stuff having to do with 'last' variable is a hack to make the
  # bottom table's bottom border appear and dissapear on opening.
  $('h3.blur_table').live 'click', ->
    id = $(this).attr('id')
    last = id == $('h3.blur_table').filter(':last').attr('id')
    if last and $(this).css('border-bottom-width') isnt '0px'
      $(this).css('border-bottom-width','0px')
      last = false
    $('div#' + id).slideToggle 'fast', ->
      # Hack to remove border on last header element while open
      if last and $('#' + id).css('border-bottom-width') is '0px'
        $('#' + id).css('border-bottom-width', '1px')

  # Function to initialize a filter tree on the passed in element
  setup_filter_tree = (selector) ->
    selector.jstree
      plugins: ["themes", "html_data", "sort", "ui"],
      themes:
        theme: 'apple',
        icons: false,
    .bind "select_node.jstree", (event, data) -> 
      $(this).jstree('toggle_node')
    $('.host_list').bind("loaded.jstree", ->
      $('.host_list').show()
    )
    $('.schema_list').bind("loaded.jstree", ->
      $('.schema_list').show()
    )

  # Calls the function to initialize the filter tree on the schema list
  setup_filter_tree $('.schema_list')
  setup_filter_tree $('.host_list')

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
      setup_filter_tree($('.schema_list'))
    .live 'ajax:error', (evt, xhr, status, error) ->
      console.log "Error in update ajax call"
    
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
