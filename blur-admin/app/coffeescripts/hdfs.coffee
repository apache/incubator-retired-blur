$(document).ready ->
  # Method to initialize the jstree
  setup_file_tree = () ->
    $('.hdfs-node').contextMenu
      menu: 'hdfs-context-menu',
      (action, el, pos) ->
        perform_action action, el
    $('.file_layout').jstree
      plugins: ["themes", "html_data", "sort", "ui", "search" ],
      themes:
        theme: 'apple',
    $('.file_layout').bind "loaded.jstree", ->
      $('#hdfs_files').show()  
    $('#search_button').live "click", ->
      search_file_tree()
    $('#search_string').live "keypress", (name) ->
      if name.keyCode == 13 && !name.shiftKey   #check if the key is enter
        name.preventDefault()
        search_file_tree()
    $('.file_layout').bind "search.jstree", (e, data) ->
      array = {}
      array['search_string'] = data.rslt.str
      $.each data.rslt.nodes, -> 
        array[$('li#' + this.id).attr('name')] = $('a#' + this.id).attr 'connection'
      search_results array
      back_history.push array
      $('#back_button').button 'enable'
      $('#up_button').button 'disable'
      $('#forward_button').button 'disable'

  tree_context_menu = () ->
    $("<ul id='hdfs-context-menu' class='contextMenu'>
      <li class='cut'><a href='#cut'>Cut</a></li>
      <li class='copy'><a href='#copy'>Copy</a></li>
      <li class='paste'><a href='#paste'>Paste</a></li>
      <li class='delete'><a href='#delete'>Delete</a></li>
    ")
  search_file_tree = () ->
    $('.jstree-search').removeClass 'jstree-search'
    $('.file_layout').jstree "search", $('#search_string').val()

  search_results = (array) ->
    $.post '/hdfs/search', { 'results': array }, (data) ->
      $('#data_container_display').html data
      change_view()
      $('#search_string').val array['*search_string*']
      $.each $("#file_tiles > button" ), -> $('#file_tiles #' + this.id).button()
      
  delete_location = (location) ->
    'Fix me'
    
  move_location = (location) ->
    'Fix me'
    
  destroy_history_stack = () ->
    'Fix me'
  
  perform_action = (action, el) ->
    switch action
      when "delete"
        #destroy the stack
        alert "This was deleted"
      when "cut"
        #destroy the stack
        alert "This was cut"
      when "copy"
        #destroy the stack
        alert "This was copied"
      when "paste"
        #destroy the stack
        alert "This was pasted"
    paste_buffer.action = action
    console.log paste_buffer.action
    
  # Method to change file view
  change_view = () ->
    view = $('input:radio:checked').val()
    if view == 'list'
      $('#file_tiles, #file_details').hide()
      $('#file_list').show()
    else if view == 'icon'
      $('#file_list, #file_details').hide()
      $('#file_tiles').show()
    else if view == 'detail'
      $('#file_list, #file_tiles').hide()
      $('#file_details').show()

  # Method to display information for new file
  new_data = () ->
    $('.jstree-search').removeClass 'jstree-search'
    if back_history.length > 0
      $('#back_button').button 'enable'
      id = back_history[back_history.length - 1]
      if id == ""
        no_file()
      else if typeof(id) != 'string'
        search_results id
      else
        file = $('#'+ id).attr('name')
        connection = $('a#'+ id).attr 'connection'
        $.post '/hdfs/files', { 'file': file, 'connection': connection}, (data) ->
          $('#data_container_display').html data
          change_view()
          $.each $("#file_tiles > button" ), -> $('#file_tiles #' + this.id).button()
          $('#data_container_display .hdfs-node').contextMenu
            menu: 'hdfs-context-menu',
            (action, el, pos) ->
              perform_action action, el

        $('#location_string').val $('#'+ id).attr 'name'
        $('#up_button').button 'enable'
        $('.file_layout').jstree "open_node", '#' + id
        $('.file_layout').find('li > #' + id).addClass 'jstree-search'
        $('#search_string').val ""
    else
      $('#back_button').button 'disable'
      no_file()
    if forward_history.length > 0
      $('#forward_button').button 'enable'
    else
      $('#forward_button').button 'disable'

  # Called when no files are available to display
  no_file = () ->
    $('#up_button').button 'disable'
    $('#data_container_display').html '<div></div>'
    $('#location_string').val ''

  # Called when a new file location is called to display
  to_new_file = (id) ->
    back_history.push id
    forward_history = []
    new_data()

  # Method for file text submit
  go_to_file = () ->
    id = $('#location_string').val().replace(/[.,_:\/]/g,"-")
    if id != "" and $('#hdfs_files').find('#' + id).length > 0
      to_new_file id
    else
      $('#data_container_display').html '<div>Not a valid file location</div>'

  # Methods to call on page load
  $(document.body).append(tree_context_menu())
  setup_file_tree()
  #set up the buttons
  $('#view_options').buttonset()
  $.each $("#toolbar button,#toolbar input[type='submit']"), -> $('#toolbar #' + this.id).button()
  #variables to help with history, and the context menu buffer
  back_history = []; forward_history = []; paste_buffer = [];

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
    parent = $('#' + back_history[back_history.length - 1]).parent().attr 'class'
    if !parent
      parent = ''
    to_new_file parent

  # Listener for file view option
  $('#view_options').live 'change', ->
    view = $('#view_options').find(':checked').attr 'value'
    change_view()

  # Listener for file text submit on 'go'
  $('#go_button').live 'click', ->
    go_to_file()

  # Listener for file text submit on enter
  $('#location_string').live "keypress", (name) ->
    if name.keyCode == 13 && !name.shiftKey   #check if it is enter
      name.preventDefault()
      go_to_file()
