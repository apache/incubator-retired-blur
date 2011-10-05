$(document).ready ->
  # Method to initialize the jstree
  setup_file_tree = () ->
    $('#hdfs_instances').bind 'loaded.jstree', () ->
      $('.hdfs_root').contextMenu
        menu: 'hdfs-root-context-menu',
        (action, el, pos) ->
          perform_action action, el
          
    $('#hdfs_instances').jstree
      plugins: ["themes", "json_data", "sort", "ui", "search"],
      themes:
        theme: 'apple',
      json_data:
        data: hdfs_tree_data,
        ajax:
          url: '/hdfs/expand'
          data: (node) ->
            info = node.data()
            {'hdfs':info.hdfs_id, 'fs_path':info.fs_path}
          success: (nodes) ->
            $('.hdfs_dir').destroyContextMenu()
            $('.hdfs_file').destroyContextMenu()
            nodes
          complete: () ->
            $('.hdfs_dir').contextMenu
              menu: 'hdfs-dir-context-menu',
              (action, el, pos) ->
                perform_action action, el
            $('.hdfs_file').contextMenu
              menu: 'hdfs-file-context-menu'
              (action, el, pos) ->
                perform_action action, el

  tree_context_menu = () ->
    $("<div class='context_menus'>
      <ul id='hdfs-root-context-menu' class='contextMenu'>
      <li class='props separator'><a href='#props'>Properties</a></li>
      </ul>
      <ul id='hdfs-dir-context-menu' class='contextMenu'>
      <li class='cut'><a href='#cut'>Cut</a></li>
      <li class='paste'><a href='#paste'>Paste</a></li>
      <li class='delete'><a href='#delete'>Delete</a></li>
      </ul>
      <ul id='hdfs-file-context-menu' class='contextMenu'>
      <li class='cut'><a href='#cut'>Cut</a></li>
      <li class='paste'><a href='#paste'>Paste</a></li>
      <li class='delete'><a href='#delete'>Delete</a></li>
      </ul>
      </div>
    ")
          
  cut_file = (file, location) ->
    file_data = $(file).data()
    location_data = $(location).data()
    if file_data.hdfs_id == location_data.hdfs_id
      target_file = file_data.fs_path
      target_location = location_data.fs_path
      $.post '/hdfs/cut_file', { 'target': target_file, 'location': target_location, 'hdfs': file_data.hdfs_id}
    
  delete_file = (file) ->
    data = $(file).data()
    $.post '/hdfs/delete_file', { 'fs_path': data.fs_path, 'hdfs': data.hdfs_id}
  
  perform_action = (action, el) ->
    switch action
      when "delete"
        delete_file(el)
      when "cut"
        paste_buffer.location = el
        paste_buffer.action = action
      when "paste"
        if paste_buffer.action
          if paste_buffer.action == "cut"
            cut_file(paste_buffer.location, el)
    
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

  view_node = (node) ->
    data = node.data()
    $('#data_container_display').load "/hdfs/view_node?hdfs=#{data.hdfs_id}&fs_path=#{data.fs_path}&view_type=#{$('input:radio:checked').val()}", () ->
      $('.view_hdfs_dir').contextMenu
        menu: 'hdfs-dir-context-menu',
        (action, el, pos) ->
          perform_action action, el
      $('.view_hdfs_file').contextMenu
        menu: 'hdfs-file-context-menu'
        (action, el, pos) ->
          perform_action action, el
      change_view()

  # Methods to call on page load
  $(document.body).append(tree_context_menu())
  setup_file_tree()
  paste_buffer = {}
  #set up the buttons
  $('#view_options').buttonset()
  $.each $("#toolbar button,#toolbar input[type='submit']"), ->
    $('#toolbar #' + this.id).button()

  # Listener for changing between the different layouts
  $('#view_options').live 'change', ->
    change_view()
    
  $('#hdfs_instances li, .view_hdfs_dir, .view_hdfs_file').live 'click', ->
    view_node $(this)