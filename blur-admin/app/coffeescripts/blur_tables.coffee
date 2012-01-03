$(document).ready ->
  $('#blur_tables').tabs()
  
  #Custom Accordian code
  $('.table_accordion .accordion-header').live 'click', (e)->
    content = $(this).next()
    if content.is(':hidden')
      $('.accordion-content').hide(500)
      content.show(500)
    $('.table_accordion .accordion-header').removeClass('selected')
    $(this).addClass('selected')
    
  
  reload_table_info = (cluster, state, shouldRepeat) ->
    
    $('#cluster_' + cluster + ' .table_accordion .' + state + '_tables').load "#{Routes.reload_blur_tables_path()}?status=#{state}&cluster_id=#{cluster}", ->
        if shouldRepeat
          setTimeout('window.reload_table_info("' + cluster + '","' + state + '",' + shouldRepeat + ')', 5000);
  window.reload_table_info = reload_table_info
  
  $('.cluster').each ->
    id = $(this).data('cluster_id')
    reload_table_info id, 'active',true
    reload_table_info id, 'disabled',true
    reload_table_info id, 'deleted',true

  # Function to initialize a filter tree on the passed in element
  setup_filter_tree = (selector) ->
    selector.jstree
      plugins: ["themes", "html_data", "sort", "ui"],
      themes:
        theme: 'apple',
        icons: false,
    .bind "select_node.jstree", (event, data) -> 
      $(this).jstree('toggle_node')

  # Ajax request handling for hosts/schema link
  $('a.hosts, a.schema')
    .live 'ajax:success', (evt, data, status, xhr) ->
      title = $(this).attr('class')
      $(data).hide()
      $(data).dialog
        modal: true
        draggable: false
        resizable: false
        width: 'auto'
        title: title.substring(0,1).toUpperCase() + title.substring(1)
        close: (event, ui) ->
          $(this).remove()
        open: ->
          $(this).children().hide()
          setup_filter_tree $(this)
          $(this).children().show()
    .live 'ajax:error', (evt, xhr, status, error) ->
      # TODO: improve error handling

  # Ajax request handling for enable/disable/delete
  $('form.update, form.delete')
    .live 'ajax:beforeSend', (evt, xhr, settings) ->
      $(this).find('input[type=button]').attr('disabled', 'disabled')
    .live 'ajax:complete', (evt, xhr, status) ->
      $(this).find('input[type=button]').removeAttr('disabled')
      

  # Listener for delete button (launches dialog box)
  $('.delete_blur_table_button').live 'click', ->
    table = $(this).parents('.blur_table')
    global = table.length <= 0
    cluster_id = $(this).attr('blur_cluster_id')
    if global
      route = Routes.delete_all_blur_tables_path()
    else
      table_id = table.attr('blur_table_id')
      route = Routes.blur_table_path(table_id)
    
    confirm_msg = if global then 'Do you want to delete all of the underlying table indicies?' else 'Do you want to delete the underlying table index?'
    title = if global then 'Delete All Tables' else 'Delete Table'
    button_1 = if global then 'Delete tables and indicies' else 'Delete table/index'
    button_2 = if global then 'Delete tables only' else 'Delete table only'
    
    delete_table = (route, cluster_id, delete_index)->
      $.ajax
        url: route,
        type: 'DELETE',
        data:
          cluster_id: cluster_id
          delete_index: delete_index
          
    btns = new Array()
    btns[button_1] = ->
      delete_table(route,cluster_id,true)
      $('#modal').modal('hide')
    btns[button_2] = ->
      delete_table(route,cluster_id,false)
      $('#modal').modal('hide')
    btns["Cancel"] = ->
      $('#modal').modal('hide')
    $().popup(title,confirm_msg,btns)

  # Listener for disable button (launches dialog box)
  $('.disable_table_button').live 'click', ->
    #array of buttons, so that they are dynamic
    btns = new Array()
    btns["Disable"] = -> 
      $.ajax
        url: route,
        type: 'PUT',
        data:
          cluster_id: cluster_id
          disable: true
      $('#modal').modal('hide')
    btns["Cancel"] = -> 
      $('#modal').modal('hide')
    cluster_id = $(this).attr('blur_cluster_id')
    table = $(this).parents('.blur_table')
    global = table.length <= 0
    if global
      route = Routes.update_all_blur_tables_path()
    else
      table_id = table.attr('blur_table_id')
      route = Routes.blur_table_path(table_id)
    title = if global then 'Disable All Tables' else 'Disable Table'
    confirm_msg = if global then 'Are you sure you want to disable all of the tables?' else 'Are you sure you want to disable this table?'
    $().popup(title,confirm_msg,btns)
    
  # Listener for forget button (launches dialog box)
  $('.forget_blur_table_button').live 'click', ->
    btns = new Array()
    btns["Forget"] = -> 
      $.ajax
        url: route,
        type: 'DELETE',
        data:
          cluster_id: cluster_id
      $('#modal').modal('hide')
    btns["Cancel"] = -> 
      $('#modal').modal('hide')
    cluster_id = $(this).attr('blur_cluster_id')
    table = $(this).parents('.blur_table')
    global = table.length <= 0
    if global
      route = Routes.forget_all_blur_tables_path()
    else
      table_id = table.attr('blur_table_id')
      route = Routes.forget_blur_table_path(table_id)
    title = if global then 'Forget All Tables' else 'Forget Table'
    confirm_msg = if global then 'Are you sure you want to forget all tables?' else "Are you sure you want to disable this table?"
    $().popup(title,confirm_msg,btns)   
        
  # Listener for disable button (launches dialog box)
  $('.enable_table_button').live 'click', ->
    btns = new Array()
    btns["Enable"] = -> 
      $.ajax
        url: route,
        type: 'PUT',
        data:
          cluster_id: cluster_id
          enable: true
      $('#modal').modal('hide')
    btns["Cancel"] = -> 
      $('#modal').modal('hide')
    cluster_id = $(this).attr('blur_cluster_id')
    table = $(this).parents('.blur_table')
    global = table.length <= 0
    if global
      route = Routes.update_all_blur_tables_path()
    else
      table_id = table.attr('blur_table_id')
      route = Routes.blur_table_path(table_id)
    title = if global then 'Enable All Tables' else 'Enable Table'
    confirm_msg = if global then 'Are you sure you want to enable all of the tables?' else 'Are you sure you want to enable this table?'
    $().popup(title,confirm_msg,btns)
