$(document).ready ->
  if typeof(history.pushState) == 'undefined'
    history.pushState = ()->
  headerHeight = 0
  footerHeight = 0
  windowLoaded = () ->
    headerHeight = parseInt($('#top').css('height'), 10)
    footerHeight = parseInt($('#ft').css('height'), 10)
    $('#hdfs_wrapper').css('height', window.innerHeight - (footerHeight + headerHeight))
  window.onload = windowLoaded
  prevHeight = window.innerHeight
  $(window).resize ()->
    if prevHeight != window.innerHeight
      $('#hdfs_wrapper').css('height', window.innerHeight - (footerHeight + headerHeight))
    prevHeight = window.innerHeight
      
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
      <li class='edit' ><a href='#upload'>Upload File</a></li>
      <li class='props separator'><a href='#props'>Properties</a></li>
      </ul>
      <ul id='hdfs-dir-context-menu' class='contextMenu'>
      <li class='mkdir'><a href='#mkdir'>New Folder</a></li>
      <li class='edit' ><a href='#upload'>Upload File</a></li>
      <li class='rename'><a href='#rename'>Rename</a></li>
      <li class='cut'><a href='#cut'>Cut</a></li>
      <li class='paste'><a href='#paste'>Paste</a></li>
      <li class='delete'><a href='#delete'>Delete</a></li>
      <li class='props separator'><a href='#dirprops'>Properties</a></li>
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
    $('<div id="newName"><input></input></div>').popup
      title: 'New Name'
      titleClass: 'title'
      shown: ()->
        $('#newName input').focus()
      btnClasses:
        "Create": "primary"
      btns:
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
          $().closePopup()
        "Cancel": () ->
          $().closePopup()
    
  delete_file = (file) ->
    id = file.attr('hdfs_id');
    path = file.attr('hdfs_path');
    if(confirm("Are you sure you wish to delete " + path + "? This action can not be undone."))
      $.post Routes.hdfs_delete_path(id), { 'path': path}
      
  window.uploading = false
  finishUploading = (path)->
    $("li[hdfs_path='" + path + "']").click();
    $().closePopup()
    window.uploading = false
  window.finishUploading = finishUploading
  uploadFailed = (error)->
    $('#upload-file').html(error)
    window.uploading = false
  window.uploadFailed = uploadFailed
  upload = (el) ->
    id = el.attr('hdfs_id');
    path = el.attr('hdfs_path');
    $.get Routes.hdfs_upload_form_path(), (data)->
      $().popup
        body:data
        title: 'Upload File'
        titleClass: 'title'
        show: ()->
          $('#fpath-input').val(path)
          $('#hdfs-id-input').val(id)
        hide: ()->
          !window.uploading
  $('#upload-form').live 'submit', ()->
    window.uploading = true
    $('#upload-file #status').html '<h2>Uploading...</h2>'
    $('#upload-file #upload-button').attr('disabled','disabled')
  make_dir = (el) ->
    id = el.attr('hdfs_id')
    path = el.attr('hdfs_path');
    $('<div id="newFolder"><input></input></div>').popup
      title: 'New Folder'
      titleClass: 'title'
      shown: ()->
        $('#newFolder input').focus()
      btnClasses:
        "Create": "Primary"
      btns:
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
          $().closePopup()
        "Cancel": () ->
          $().closePopup()
          
  draw_radial_graph = (width, height, json) ->
    radius = Math.min(width, height) / 2
    color = d3.scale.category20c()
    selector = ".radial-graph"
    $(selector).empty()
    vis = d3.select(selector)
      .append("svg")
        .attr("width", width)
        .attr("height", height)
      .append("g")
        .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

    partition = d3.layout.partition()
      .sort(null)
      .size([2 * Math.PI, radius * radius])
      .value (d) ->
        return 1

    arc = d3.svg.arc()
      .startAngle((d) -> return d.x)
      .endAngle((d) -> return d.x + d.dx)
      .innerRadius((d) -> return Math.sqrt(d.y))
      .outerRadius((d) -> return Math.sqrt(d.y + d.dy));

    path = vis.data([json]).selectAll("path")
      .data(partition.nodes)
      .data(partition.value((d) -> d.size))
      .enter().append("path")
      .attr("display", (d) -> return if d.depth then null else "none")
      .attr("d", arc)
      .attr("fill-rule", "evenodd")
      .style("stroke", "#fff")
      .style("fill", (d) -> return color((if d.children then d else d.parent).name))
      .attr("title", (d) -> return d.name )
    path.data()
    $('path').hover

  show_hdfs_props = (el) ->
    id = el.attr('hdfs_id')
    title = "HDFS Information (#{el.attr('hdfs_name')})"
    $.get Routes.hdfs_info_path(id), (data) ->
      $(data).popup
        title: title
        titleClass: 'title'
        show: () ->
          $.get Routes.hdfs_structure_path(id),{'fs_path':'/'},(data) ->
            draw_radial_graph(520, 400, data)
          $('#modal').css
            'width':'1120px'
            'margin-left':'-560px'

  show_dir_props = (el) ->
    id = el.attr('hdfs_id')
    path = el.attr('hdfs_path')
    title = "Properties for #{path}"
    $.get Routes.hdfs_folder_info_path(id),{'fs_path':path},(data) ->
      $(data).popup
        titleClass: 'title'
        title: title
        show: () ->
          $.get Routes.hdfs_slow_folder_info_path(id),{'fs_path':path},(data) ->
            $('#file_count').html(data.file_count)
            $('#folder_count').html(data.folder_count)
            $('#file_size').html(data.file_size)
          $.get Routes.hdfs_structure_path(id),{'fs_path':path},(data) ->
            draw_radial_graph(520, 400, data)            
          $('#modal').css
            'width':'1120px'
            'margin-left':'-560px'

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
      when "dirprops"
        show_dir_props el
      when "mkdir"
        make_dir el
      when "rename"
        rename el
      when "upload"
        upload el
  ###
  #Methods for HTML History manipulation
  ###
  
  #selects the correct hdfs instance based on the current URL
  setHdfs = () ->
    #checks if URL is a show
    urlCheck = location.pathname.match(/^\/hdfs\/\d+\/show/)
    if urlCheck != null and urlCheck.length == 1
      #retrieves the ID from the url
      idRegex = location.pathname.match(/^\/hdfs\/(\d+)\/show/)
      if idRegex.length && idRegex.length == 2
        id = idRegex[1]
        #variable used by the added event listener, weither or not it should keep parsing through the URL
        window.hdfs_path_set = false
        $('.hdfs_instance[hdfs_id=' + id + ']').click();
      else
        window.hdfs_path_set = true
    else
      window.hdfs_path_set = true 
      
  #selects the folder on the filesystem with the given hdfs_path, if it exists
  selectFolder = (path) ->
    #retrieves HDFS id from the url
    idRegex = location.pathname.match(/^\/hdfs\/(\d+)\/show/)
    if idRegex.length && idRegex.length == 2
      id = idRegex[1]
      selector = '.osxSelectable[hdfs_path="' + path + '"][hdfs_id=' + id + ']'
      folder = $(selector)
      folder.click(); 
      
  #popstate event listener
  window.onpopstate = (e)->
    #only run if there is a previous path stored
    if window.previous_path
      #previous is contained in current, add folder
      if location.pathname.indexOf(window.previous_path) == 0
        #retrieve the path from the URL
        pathRegex = location.pathname.match(/^\/hdfs\/\d+\/show(.+)/)
        if pathRegex.length and pathRegex.length == 2
          path = pathRegex[1]
          #if the path ends in a trailing slash, remove it
          if path.charAt(path.length - 1) == "/"
            path = path.substring(0,path.length - 1)
          #prevents a double push state
          window.dont_push_state = true
          selectFolder(path)
          
      #current is contained in previous, remove current folder
      else if(window.previous_path.indexOf(location.pathname) == 0)
        $('.innerWindow').last().remove()
        $('.osxSelected').last().removeClass('osxSelected')
        
      #path is not withen a step of each other, full retread
      else
        #remove every innerWindow except for the base and any selected hdfs system
        $('.innerWindow:not(.innerWindow:eq(0))').remove()
        $('.osxSelected').removeClass('osxSelected')
        setHdfs()
    #set the previous path to the current path for the next popstate
    window.previous_path = location.pathname
    
  ###
  # Methods to call on page load
  ###
  $(document.body).append(tree_context_menu())
  setup_context_menus();
  paste_buffer = {}
  $('#hdfs_browser').osxFinder
    done:()->
      setHdfs()     
    added:(e,data)->
      #verifys that the URL was an expand
      urlCheck = data.url.match(/^\/hdfs\/\d+\/expand/)
      if urlCheck and urlCheck.length == 1
        #replace with a show URL
        current_url = data.url.replace "expand","show"
        #if was just a single add, push the history state
        if window.hdfs_path_set
          if not window.dont_push_state
            history.pushState {},'',current_url
          else window.dont_push_state = false
        else
          #parse the remainder of the URL vs what the returned path was
          urlLeft = location.pathname.substring(current_url.length)
          next_path = urlLeft.substring(0,urlLeft.indexOf('/'))
          if next_path == ""
            next_path = urlLeft
          #if the remaining path is not empty
          if next_path != ""
            pathRegex = current_url.match(/^\/hdfs\/\d+\/show(.+)/)
            if pathRegex.length and pathRegex.length == 2
              path = pathRegex[1] + next_path
              selectFolder(path)
          else
            window.hdfs_path_set = true
      else
        urlCheck = data.url.match(/^\/hdfs\/\d+\/file_info/)
        if urlCheck and urlCheck.length == 1
          if not window.dont_push_state
            current_url = data.url.replace "file_info","show"
            history.pushState {},'',current_url
          else window.dont_push_state = false
          
      #set the previous path as the current path for the next popstate
      window.previous_path = location.pathname


    
  $('#hdfs_wrapper').resizable
    handles:'s'
    stop: () ->
      $(this).css('width', '')

