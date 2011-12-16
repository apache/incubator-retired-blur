$(document).ready ->
  $('#blur_tables').tabs()
  $('.table_accordion').accordion
    collapsible: true
    autoHeight: false
  $('#blur_tables').bind "tabsshow", (event, ui)->
    $(ui.panel).find('.table_accordion').accordion('refresh')
  
  reload_table_info = (cluster, state) ->
    $('#cluster_' + cluster + ' .' + state + '_tables').load "#{Routes.reload_blur_tables_path()}?status=#{state}&cluster_id=#{cluster}", ->
      $(this).parents('.table_accordion').accordion('refresh')
      setTimeout(->
        reload_table_info(cluster, state)
      10000)
  
  $('.ui-tabs-panel').each ->
    id = $(this).data('cluster_id')
    reload_table_info id, 'active'
    reload_table_info id, 'disabled'
    reload_table_info id, 'deleted'

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
    form = $(this).closest 'form.delete'
    global_delete = form.find('.cluster_id').size() > 0
    
    confirm_msg = if global_delete then 'Do you want to delete all of the underlying table indicies?' else 'Do you want to delete the underlying table index?'
    title = if global_delete then 'Delete All Tables' else 'Delete Table'
    button_1 = if global_delete then 'Delete tables and indicies' else 'Delete table/index'
    button_2 = if global_delete then 'Delete tables only' else 'Delete table only'
    
    buttons_map = {}
    buttons_map[button_1] = ->
      form.find('.delete_index').val 'true'
      form.submit()
      $(this).dialog 'close'
    buttons_map[button_2] = ->
      form.submit()
      $(this).dialog 'close'
    buttons_map["Cancel"] = ->
      $(this).dialog 'close'
      
    $("<div class='confirm_delete'>#{confirm_msg}</div>").dialog
      width: 'auto',
      modal: true,
      draggable: false,
      resizable: false,
      title: title,
      buttons:buttons_map,
      close: ->
        $(this).remove()

  # Listener for disable button (launches dialog box)
  $('.disable_table_button').live 'click', ->
    #array of buttons, so that they are dynamic
    btns = {}
    btns[$(this).val()] = -> 
      form.submit()
      $(this).dialog 'close'
    btns["Cancel"] = -> 
      $(this).dialog 'close'
      
    form = $(this).closest 'form.update'
    global_delete = form.find('.cluster_id').size() > 0
    
    confirm_msg = if global_delete then 'Are you sure you want to disable all of the tables?' else 'Are you sure you want to disable this table?'
    $("<div class='confirm_enable_disable'>#{confirm_msg}</div>").dialog
      modal: true,
      draggable: false,
      resizable: false,
      buttons: btns,        
      close: ->
        $(this).remove()
        
  # Listener for forget button (launches dialog box)
  $('.forget_blur_table_button').live 'click', ->
    #array of buttons, so that they are dynamic
    btns = {}
    _self = $(this)
    btns[_self.val()] = ->
      form.submit()
      $(this).dialog 'close'
      console.log(_self.parents('tr:first'))
      _self.closest('tr').hide()
    btns["Cancel"] = -> 
      $(this).dialog 'close'

    form = $(this).closest 'form.delete'

    confirm_msg = 'Are you sure you want to forget this table?'
    $("<div class='confirm_forget'>#{confirm_msg}</div>").dialog
      modal: true,
      draggable: false,
      resizable: false,
      buttons: btns,        
      close: ->
        $(this).remove()

  # Listener for forget all button (launches dialog box)
  $('.forget_all_tables_button').live 'click', ->
    #array of buttons, so that they are dynamic
    btns = {}
    _self = $(this)
    btns[_self.val()] = ->
      form.submit()
      $(this).dialog 'close'
      _self.closest('div.deleted_tables').find('tbody tr').hide()
    btns["Cancel"] = ->
      $(this).dialog 'close'

    form = $(this).closest 'form.delete'

    confirm_msg = 'Are you sure you want to forget all the table?'
    $("<div class='confirm_forget'>#{confirm_msg}</div>").dialog
      modal: true,
      draggable: false,
      resizable: false,
      buttons: btns,
      close: ->
        $(this).remove()
        
  # Listener for enable button (launches dialog box)
  $('.enable_table_button').live 'click', ->
    #array of buttons, so that they are dynamic
    btns = {}
    btns[$(this).val()] = -> 
      form.submit()
      $(this).dialog 'close'
    btns["Cancel"] = -> 
      $(this).dialog 'close'

    form = $(this).closest 'form.update'
    global_delete = form.find('.cluster_id').size() > 0
    
    confirm_msg = if global_delete then 'Are you sure you want to enable all of the tables?' else 'Are you sure you want to enable this table?'
    $("<div class='confirm_enable_disable'>#{confirm_msg}</div>").dialog
      modal: true,
      draggable: false,
      resizable: false,
      buttons: btns,        
      close: ->
        $(this).remove()
