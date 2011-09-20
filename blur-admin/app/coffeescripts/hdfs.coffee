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
      add_to_back $('#data_container_display').html()
      search_results array
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
      $('#data_container_display > .file_id').attr('data-search', array['search_string'])
      change_view()
      $('#search_string').val array['search_string']
      $.each $("#file_tiles > button" ), -> $('#file_tiles #' + this.id).button()
      
  copy = (location) ->
    'Fix me'
    #invalidate forward and back stacks
    
  paste = (location) ->
    'Fix me'
    #invalidate forward and back stacks
    
  delete = (location) ->
    'Fix me'
    #invalidate forward and back stacks
  
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
  
  add_to_back = (hist_el) ->
    if hist_el != ""
      back_history.push hist_el
      $('#back_button').button 'enable'
    
  add_to_forward = (hist_el) ->
    if hist_el != ""
      forward_history.push hist_el
      $('#forward_button').button 'enable'
    
  # Method to change file view
  change_view = () ->
    switch $('input:radio:checked').val()
      when 'list'
        $('#file_tiles, #file_details,#file_list').hide()
        $('#file_list').show()
      when 'icon'
        $('#file_list, #file_details').hide()
        $('#file_tiles').show()
      when 'detail'
        $('#file_list, #file_tiles').hide()
        $('#file_details').show()

  set_view_state = ->
    #if the new view we are showing has a definite location find it and set it in the location
    def_location = $('#data_container_display > .file_id').attr('id')
    $('.file_layout').jstree 'close_all'
    if def_location
      $('#location_string').val $('#' + def_location).attr 'name'
      $('#up_button').button 'enable'
      $('.file_layout').jstree "open_node", '#' + def_location
      $('.file_layout').find('li > #' + def_location).addClass 'jstree-search'
      $('#search_string').val ""
    #else it is an old search and we can show the string
    else
      $('#search_string').val $('#data_container_display > .file_id').attr('data-search')

  # Method for search text submit
  display_file_at_path = (id) ->
    forward_history = []
    $('#forward_button').button 'disable'
    $('.jstree-search').removeClass 'jstree-search'
    if !id
      id = $('#location_string').val().replace(/[.,_:\/]/g,"-")
    if id != "" and $('#hdfs_files').find('#' + id).length > 0
      file = $('#'+ id).attr('name')
      connection = $('a#'+ id).attr 'connection'
      $.post '/hdfs/files', { 'file': file, 'connection': connection}, (data) ->
        add_to_back $('#data_container_display').html()
        $('#data_container_display').html data
        $('#location_string').val(file)
        set_view_state()
        change_view()
        $.each $("#file_tiles > button" ), -> $('#file_tiles #' + this.id).button()
        $('#data_container_display .hdfs-node').contextMenu
          menu: 'hdfs-context-menu',
          (action, el, pos) ->
            perform_action action, el
    else
      $('#data_container_display').html '<div>Not a valid file location on this system.</div>'
      $('.file_layout').jstree 'close_all'

  # Methods to call on page load
  $(document.body).append(tree_context_menu())
  setup_file_tree()
  #set up the buttons
  $('#view_options').buttonset()
  $.each $("#toolbar button,#toolbar input[type='submit']"), ->
    $('#toolbar #' + this.id).button()
  #variables to help with history, and the context menu buffer
  back_history = []; forward_history = []; paste_buffer = [];

  # Listeners for back/forward buttons
  $('#back_button').live 'click', ->
    $('.jstree-search').removeClass 'jstree-search'
    if back_history.length > 0
      add_to_forward $('#data_container_display').html()
      $('#data_container_display').html back_history.pop()
      if back_history.length <= 0
        $('#back_button').button 'disable'
      set_view_state()
    else
      $('#data_container_display').html 'No Page in the Back Queue, our mistake.'
      $('#back_button').button 'disable'
      $('.file_layout').jstree 'close_all'
      if forward_history.length < 0
        $('#forward_button').button 'enable'
        
  $('#forward_button').live 'click', ->
    $('.jstree-search').removeClass 'jstree-search'
    if forward_history.length > 0
      add_to_back $('#data_container_display').html()
      $('#data_container_display').html forward_history.pop()
      $('#back_button').button 'enable'
      if forward_history.length <= 0
        $('#forward_button').button 'disable'
      set_view_state()
    else
      $('#data_container_display').html 'No Page in the Forward Queue, our mistake.'
      $('#forward_button').button 'disable'
      $('.file_layout').jstree 'close_all'
      if back_history.length > 0
        $('#back_button').button 'enable'

  # Listener for all file links
  $('#hdfs_files a, #file_tiles > .ui-button, #file_list a, #file_details tbody tr').live 'click', ->
    display_file_at_path this.id

  # Listener for file up button
  $('#up_button').live 'click', ->
    parent = $('#' + $('#data_container_display > .file_id').attr('id')).parent().attr 'class'
    if !parent
      parent = ''
    display_file_at_path parent

  # Listener for changing between the different layouts
  $('#view_options').live 'change', ->
    change_view()

  # Listener for file search submit on 'go'
  $('#go_button').live 'click', ->
    display_file_at_path()

  # Listener for file text submit on enter
  $('#location_string').live "keypress", (name) ->
    if name.keyCode == 13 && !name.shiftKey
      name.preventDefault()
      display_file_at_path()
