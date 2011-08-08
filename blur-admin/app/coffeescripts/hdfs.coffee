$(document).ready ->
  # Method to initialize the jstree
  setup_file_tree = () ->
    $('.file_layout').jstree
      plugins: ["themes", "html_data", "sort", "ui", "search" ],
      themes:
        theme: 'apple',
    $('.file_layout').bind "loaded.jstree", ->
      $('#hdfs_files').show()
    $('#search_button').live "click", ->
      $('.file_layout').jstree "search", $('#search_string').val()
    $('.file_layout').bind "search.jstree", (e, data) ->
      alert "Found " + data.rslt.nodes.length + " nodes matching '" + data.rslt.str + "'."

  # Methods to call on page load
  setup_file_tree()
  $('#view_options').buttonset()
  $.each($("#toolbar button,#toolbar input[type='submit']" ), ->
    $('#toolbar #' + this.id).button()
  )
  back_history = []
  forward_history = []

  # Method to change file view
  change_view = () ->
    view = $('input:radio:checked').val()
    if view == 'list'
      $('#file_tiles').hide()
      $('#file_list').show()
      $('#file_details').hide()
    else if view == 'icon'
      $('#file_list').hide()
      $('#file_tiles').show()
      $('#file_details').hide()
    else if view == 'detail'
      $('#file_list').hide()
      $('#file_tiles').hide()
      $('#file_details').show()

  # Method to display information for new file
  new_data = () ->
    if back_history.length > 0
      $('#back_button').button('enable')
      id = back_history[back_history.length - 1]
      if id == ""
        no_file()
      else
        file = $('#'+ id).attr('name').replace(/\//g," ").replace('.','*')
        connection = $('#'+ id).attr('connection')
        $.ajax '/hdfs/' + file + '/' + connection,
          type: 'POST',
          success: (data) ->
            $('#data_container_display').html data
            change_view()
            $.each($("#file_tiles > button" ), ->
              $('#file_tiles #' + this.id).button()
            )
        $('#location_string').val $('#'+ id).attr('name')
        $('#up_button').button('enable')
    else
      $('#back_button').button('disable')
      no_file()
    if forward_history.length > 0
      $('#forward_button').button('enable')
    else
      $('#forward_button').button('disable')

  # Called when no files are available to display
  no_file = () ->
    $('#up_button').button('disable')
    $('#data_container_display').html '<div></div>'
    $('#location_string').val ''

  # Called when a new file location is called to display
  to_new_file = (id) ->
    back_history.push id
    forward_history = []
    new_data()

  # Listeners for back/forward buttons
  $('#back_button').live 'click', ->
    forward_history.push back_history.pop()
    new_data()
  $('#forward_button').live 'click', ->
    back_history.push forward_history.pop()
    new_data()

  # Listener for all file links
  $('#hdfs_files a, #file_tiles > .ui-button, #file_list a, #file_details tbody tr').live 'click', ->
    to_new_file this.id

  # Listener for file up button
  $('#up_button').live 'click', ->
    parent = $('#' + back_history[back_history.length - 1]).parent().attr('class')
    if !parent
      parent = ''
    to_new_file parent

  # Listener for file view option
  $('#view_options').live 'change', ->
    view = $('#view_options').find(':checked').attr('value')
    change_view()

  # Listener for file text submit on 'go'
  $('#go_button').live 'click', ->
    go_to_file()

  # Listener for file text submit on enter
  $('#location_string').live "keypress keydown keyup", (name) ->
    if name.keyCode == 13 && !name.shiftKey   #check if it is enter
      name.preventDefault()
      go_to_file()

  # Method for file text submit
  go_to_file = () ->
    id = $('#location_string').val().replace(/[.,_:\/]/g,"-")
    if id != "" and $('#hdfs_files').find('#' + id).length > 0
      to_new_file id
    else
      $('#data_container_display').html '<div>Not a valid file location</div>'
