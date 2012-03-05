#= require jquery.contextMenu
#= require jquery.osxFinder
#= require d3/d3
#= require flot/flot
#= require_self

$(document).ready ->
  # Old browser support
  if typeof(history.pushState) == 'undefined'
    history.pushState = () ->
  
  # Setup a view variables
  ( () ->
    headerHeight = 0; footerHeight = 0;
    prevHeight = window.innerHeight
    windowLoaded = () ->
      headerHeight = parseInt($('#top').css('height'), 10)
      footerHeight = parseInt($('#ft').css('height'), 10)
      newHeight = window.innerHeight - (footerHeight + headerHeight) - 10
      $('#hdfs_wrapper').animate({height: newHeight + 'px'}, 400)
    window.onload = windowLoaded
    $(window).resize ()->
      if prevHeight != window.innerHeight
        $('#hdfs_wrapper').css('height', window.innerHeight - (footerHeight + headerHeight) - 10)
      prevHeight = window.innerHeight
  )()
      
  # Context Menu setup
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
          
  # Menu Option Method Handlers  
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
    $("li[hdfs_path='" + path + "']").click()
    $().closePopup()
    window.uploading = false
  window.finishUploading = finishUploading
  
  uploadFailed = (error)->
    $('#upload-file').html(error)
    window.uploading = false
  window.uploadFailed = uploadFailed
  
  upload = (el) ->
    id = el.attr('hdfs_id')
    path = el.attr('hdfs_path')
    $.get Routes.hdfs_upload_form_path(id), (data) ->
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
    $('<div id="newFolder"><label>Folder Name:</label><input></input></div>').popup
      title: 'New Folder'
      titleClass: 'title'
      shown: ()->
        $('#newFolder input').focus()
      btns:
        "Create":
          class: 'primary'
          func: ()->
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
        "Cancel":
          func: () ->
            $().closePopup()
            
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
        show_hdfs_props el.attr('hdfs_id'), el.attr('hdfs_name')
      when "dirprops"
        show_dir_props el.attr('hdfs_id'), el.attr('hdfs_path')
      when "mkdir"
        make_dir el
      when "rename"
        rename el
      when "upload"
        upload el
          
  draw_radial_graph = (width, height, json) ->
    showGraphTooltip = (graph, tipContent) ->
      tooltip = $('<div class="graphtip" ><div id="tipcontent">' + tipContent + '</div></div>')
      $('.radial-graph').append tooltip
      graphWidth = graph.outerWidth()
      graphHeight = graph.outerHeight()
      tipWidth = tooltip.outerWidth()
      tipHeight = tooltip.outerHeight() 
      drawPositionX = (graphWidth / 2) - (tipWidth / 2)
      drawPositionY = (graphHeight / 2) - (tipHeight / 2)
      tooltip.css({top: drawPositionY+'px', left: drawPositionX+'px'})
      tooltip.fadeIn(400)

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
      .sort((a, b) -> b.size - a.size)
      .size([2 * Math.PI, radius * radius])
      .value((d) -> d.size)

    arc = d3.svg.arc()
      .startAngle((d) -> return d.x)
      .endAngle((d) -> return d.x + d.dx)
      .innerRadius((d) -> return Math.sqrt(d.y))
      .outerRadius((d) -> return Math.sqrt(d.y + d.dy));

    path = vis.data([json]).selectAll("path")
      .data(partition.nodes)
      .enter().append("path")
      .attr("display", (d) -> return if d.depth then null else "none")
      .attr("d", arc)
      .attr("fill-rule", "evenodd")
      .style("stroke", "#fff")
      .style("fill", (d) -> return color((if d.children then d else d.parent).name))
      .attr("title", (d) -> return d.name )
    timeoutShowVar = null
    $('path').hover(() ->
        title = $(this).attr('title') || "No path name found!"
        $('.graphtip').remove()
        clearTimeout(timeoutShowVar)
        timeoutShowVar = setTimeout ( -> showGraphTooltip($('.radial-graph'), title)), 500
      , () ->
        clearTimeout(timeoutShowVar)
        $('.graphtip').remove())

  show_hdfs_props = (id, name) ->
    title = "HDFS Information (#{name})"
    $('.hdfs_instance[hdfs_id=' + id + ']').click()
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

  show_dir_props = (id, path) ->
    title = "Properties for #{path}"
    $('.osxSelectable[hdfs_path="' + path + '"][hdfs_id=' + id + ']').click()
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
            
  ###
  #Methods for HTML History manipulation
  ###
  navigateUsingPath = () ->
    #navigate to the folder given in the path
    pathPieces = window.location.pathname.split('/').filter((member) ->
      return member != '').slice(1)
    hdfsId = pathPieces.shift()
    path = '/' + pathPieces.slice(1).join('/')
    $('#hdfs_browser').osxFinder('navigateToPath', path, hdfsId, true)
  
  #popstate event listener
  window.onpopstate = (e) ->
    navigateUsingPath()
    
  ###
  # Methods to call on page load
  ###
  $(document.body).append(tree_context_menu())
  setup_context_menus();
  paste_buffer = {}
  $('#hdfs_browser').resizable
    handles:'s'
    stop: () ->
      $(this).css('width', '')
  $('path').live 'click', () ->
    $().closePopup()
    id = $('#top_level .osxSelected').attr 'hdfs_id'
    fullpath = $(this).attr('title')
    fullpath = fullpath.substring(fullpath.indexOf('//') + 2)
    path = fullpath.substring(fullpath.indexOf('/'))
    $('#hdfs_browser').osxFinder('navigateToPath', path)
    show_dir_props(id, path)
    
  $('#hdfs_browser').osxFinder
    done: () ->
      navigateUsingPath()
    navigated: (e, data) ->
      history.pushState {}, '', data.url
  

