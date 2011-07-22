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

  # Close all accordions on page load
  for table in $('div.blur_table')
    $(table).hide()

  # Accordion open/close Listener
  # Stuff having to do with 'last' variable is a hack to make the
  # bottom table's bottom border appear and dissapear on opening.
  $('h3.blur_table').live 'click', ->
    id = $(this).attr('id')
    $('div#' + id).slideToggle 'fast', ->

  # Ajax request handling for hosts/schema link
  $('a#hosts, a#schema')
    .live 'ajax:success', (evt, data, status, xhr) ->
      title = $(this).attr('id')
      $(data).dialog
        modal: true
        draggable: false
        resizable: false
        width: 'auto'
        title: title.substring(0,1).toUpperCase() + title.substring(1)
        close: (event, ui) ->
          $(this).remove()
        open: ->
          $(data).hide()
          setup_filter_tree $(this)
          $(data).show()
    .live 'ajax:error', (evt, xhr, status, error) ->
      # TODO: improve error handling
      console.log "Error in ajax call"

  # Ajax request handling for enable/disable/delete
  $('form.update, form.delete')
    .live 'ajax:beforeSend', (evt, xhr, settings) ->
      $(this).find('input[type=submit]').attr('disabled', 'disabled')
    .live 'ajax:complete', (evt, xhr, status) ->
      $(this).find('input[type=submit]').removeAttr('disabled')
    .live 'ajax:success', (evt, data, status, xhr) ->
      selector = ".blur_table#" + $(this).attr 'id'
      $(selector).filter(':first').remove()
      $(selector).replaceWith data
    .live 'ajax:error', (evt, xhr, status, error) ->
      # TODO: improve error handling
      console.log "Error in ajax call"
    
  # Listener for delete button (launches dialog box)
  $('.delete_blur_table_button').live 'click', ->
    form = $(this).closest 'form.delete'
    $("<div class='confirm_delete'>Do you want to delete the underlying table index?</div>").dialog
      width: 400,
      modal: true,
      draggable: false,
      resizable: false,
      title: "Delete Table",
      buttons:
        "Delete Index": ->
          form.find('#delete_index').val 'true'
          form.submit()
          $(this).dialog 'close'
        "Preserve Index": ->
          form.submit()
          $(this).dialog 'close'
        "Cancel": ->
          $(this).dialog 'close'
      close: ->
        $(this).remove()

  # Listener for enable/disable button (launches dialog box)
  $('.enable_disable_table_button').live 'click', ->
    form = $(this).closest 'form.update'
    $("<div class='confirm_enable_disable'>Are you sure?</div>").dialog
      modal: true,
      draggable: false,
      resizable: false,
      buttons:
        "Yes": ->
          form.submit()
          $(this).dialog 'close'
        "Cancel": ->
          $(this).dialog 'close'
      close: ->
        $(this).remove()
