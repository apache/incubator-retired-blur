$(document).ready ->
  # Method to initialize the jstree
  setup_context_menus = () ->
      $('#hdfs_browser li.hdfs_instance').contextMenu
        menu: 'hdfs-root-context-menu',
        (action, el, pos) ->
          perform_action action, el
          return false
      $('#hdfs_browser li.folder').contextMenu
        menu: 'hdfs-dir-context-menu',
        (action, el, pos) ->
          perform_action action, el
          return false
      $('#hdfs_browser li.file').contextMenu
        menu: 'hdfs-file-context-menu',
        (action, el, pos) ->
          perform_action action, el
          return false

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
      $.post Routes.hdfs_cut_file_path(), { 'target': target_file, 'location': target_location, 'hdfs': file_data.hdfs_id}
    
  delete_file = (file) ->
    data = $(file).data()
    $.post hdfs_delete_file_path, { 'fs_path': data.fs_path, 'hdfs': data.hdfs_id}
    
  show_hdfs_props = (el) ->
    try
      id = el.attr('hdfs_id')
      title = "HDFS Information (#{el.attr('hdfs_name')})"
      $.get Routes.hdfs_info_path(id), (data) ->
        $(data).dialog
          modal: true
          draggable: false
          resizable: false
          width: 'auto'
          title: title
          close: (event, ui) ->
            $(this).remove()
          open: (event, ui)->
    catch error
      alert(error)
  
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
      when "props"
        show_hdfs_props el


  # Methods to call on page load
  $(document.body).append(tree_context_menu())
  setup_context_menus();
  paste_buffer = {}
  $('#hdfs_browser').osxFinder();
  $('#hdfs_wrapper').resizable({handles:'s'})
