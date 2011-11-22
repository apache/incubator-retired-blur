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
      $('#hdfs-dir-context-menu').disableContextMenuItems('#paste')
      if $('#hdfs_browser').attr('hdfs_editor') == 'false'
        $('.contextMenu').disableContextMenuItems('#paste,#mkdir,#cut,#rename,#delete')

  tree_context_menu = () ->
    $("<div class='context_menus'>
      <ul id='hdfs-root-context-menu' class='contextMenu'>
      <li class='mkdir'><a href='#mkdir'>New Folder</a></li>
      <li class='props separator'><a href='#props'>Properties</a></li>
      </ul>
      <ul id='hdfs-dir-context-menu' class='contextMenu'>
      <li class='mkdir'><a href='#mkdir'>New Folder</a></li>
      <li class='rename'><a href='#rename'>Rename</a></li>
      <li class='cut'><a href='#cut'>Cut</a></li>
      <li class='paste'><a href='#paste'>Paste</a></li>
      <li class='delete'><a href='#delete'>Delete</a></li>
      </ul>
      <ul id='hdfs-file-context-menu' class='contextMenu'>
      <li class='rename'><a href='#rename'>Rename</a></li>
      <li class='cut'><a href='#cut'>Cut</a></li>
      <li class='delete'><a href='#delete'>Delete</a></li>
      </ul>
      </div>
    ")
          
  cut_file = (file, location) ->
    from_id = file.attr('hdfs_id')
    from_path = file.attr('hdfs_path')
    to_id = location.attr('hdfs_id')
    to_path = location.attr('hdfs_path')
    if from_id == to_id
      $.post Routes.hdfs_move_path(to_id), { 'from': from_path, 'to': to_path}, ()->
        $('#hdfs-dir-context-menu').disableContextMenuItems('#paste')
      
  rename = (el) ->
    id = el.attr('hdfs_id')
    from_path = el.attr('hdfs_path')
    $('<div id="newName"><input></input></div>').dialog
      modal: true
      draggable: true
      resizable: false
      width: 'auto'
      title: 'New Name'
      open: ()->
        $('#newName input').focus()
      buttons:
        "Create": ()->
          newName = $('#newName input').val()
          newFullPath = "#{from_path.substring(0, from_path.lastIndexOf('/')+1)}#{newName}"
          $.ajax Routes.hdfs_move_path(id),
            type: 'post',
            data:
              from: from_path
              to: newFullPath
            success: () ->
              el.attr('hdfs_path', newFullPath)
              link = el.find('a')
              link.html(newName)
              href = link.attr('href')
              link.attr('href', href.replace(from_path, newFullPath))
              if(el.hasClass('osxSelected'))
                nextWin = el.parents('.innerWindow').next()
                display_href = el.find('a').attr('href')
                nextWin.load(display_href)
              else
                el.click()
          $(this).dialog("close")
        "Cancel": () ->
          $(this).dialog("close")
      close: (event, ui) ->
          $(this).remove()
    
  delete_file = (file) ->
    id = file.attr('hdfs_id');
    path = file.attr('hdfs_path');
    if(confirm("Are you sure you wish to delete " + path + "? This action can not be undone."))
      $.post Routes.hdfs_delete_path(id), { 'path': path}

  make_dir = (el) ->
    id = el.attr('hdfs_id')
    path = el.attr('hdfs_path');
    $('<div id="newFolder"><input></input></div>').dialog
      modal: true
      draggable: true
      resizable: false
      width: 'auto'
      title: 'New Folder'
      open: ()->
        $('#newFolder input').focus()
      buttons:
        "Create": ()->
          $.ajax Routes.hdfs_mkdir_path(id),
            type: 'post',
            data:
              fs_path: path
              folder: $('#newFolder input').val()
            success: () ->
              if(el.hasClass('osxSelected'))
                nextWin = el.parents('.innerWindow').next()
                display_href = el.find('a').attr('href')
                nextWin.load(display_href)
              else
                el.click()
          $(this).dialog("close")
        "Cancel": () ->
          $(this).dialog("close")
      close: (event, ui) ->
          $(this).remove()

  show_hdfs_props = (el) ->
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

  perform_action = (action, el) ->
    switch action
      when "delete"
        delete_file(el)
      when "cut"
        paste_buffer.location = el
        paste_buffer.action = action
        $('#hdfs-dir-context-menu').enableContextMenuItems('#paste')
      when "paste"
        if paste_buffer.action
          if paste_buffer.action == "cut"
            cut_file(paste_buffer.location, el)
      when "props"
        show_hdfs_props el
      when "mkdir"
        make_dir el
      when "rename"
        rename el


  # Methods to call on page load
  $(document.body).append(tree_context_menu())
  setup_context_menus();
  paste_buffer = {}
  $('#hdfs_browser').osxFinder();
  $('#hdfs_wrapper').resizable
    handles:'s'
    stop: () ->
      $(this).css('width', '')

