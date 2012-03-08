#= require jquery.jstree
$(document).ready ->
  refresh_timeout = null
  state_lookup = 
    0 : 'deleted'
    1 : 'deleting'
    2 : 'disabled'
    3 : 'disabling'
    4 : 'active'
    5 : 'enabling'  
  table_lookup = 
    0 : 'deleted'
    1 : 'disabled'
    2 : 'disabled'
    3 : 'active'
    4 : 'active'
    5 : 'disabled'
  colspan_lookup = 
    'active' : 6
    'disabled' : 4
    'deleted' : 2  
  #converts a number to a string with comma separation
  number_commas = (number) ->
    if number
      number.toString(10).replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
    else
      'Unknown'
  get_selected_tables = () ->
    $('.bulk-action-checkbox:checked').closest('tr').map ->
      return $(this).attr('blur_table_id')
  get_host_shard_info = (blur_table) ->
    server = $.parseJSON(blur_table['server'])
    if server
      hosts = 0
      count = 0
      for key of server
        hosts++
        count += server[key].length
      info =
        hosts: hosts
        shards: count
    else
      info =
        hosts: 'Unknown'
        shards: 'Unknown'
  build_table_row = (blur_table) ->
    capitalize_first = (string) ->
      string.charAt(0).toUpperCase() + string.slice(1)
    state = state_lookup[blur_table['status']]
    table = table_lookup[blur_table['status']]
    id = blur_table['id']
    host_info = get_host_shard_info(blur_table)
    row = $("<tr class='blur_table' blur_table_id='#{id}'></tr>")
    row.data('status', state)
    if ['disabling', 'enabling', 'deleting'].indexOf(state) >= 0
      col_span = colspan_lookup[table]
      row.append("<td colspan='#{col_span}'>#{capitalize_first(state) + ' ' +blur_table['table_name']}...</td>")
    else
      row.append("<td><input class='bulk-action-checkbox' type='checkbox'/></td><td class='blur_table_name'>#{blur_table['table_name']}</td>")        
      if table == 'active'
        host_html = "<td class='blur_table_hosts_shards'>"
        if blur_table['server']
          host_html += "<a class='hosts' href='#{Routes.hosts_blur_table_path(id)}' data-remote='true'>#{host_info['hosts']} / #{host_info['shards']}</a>"
        else
          host_html += "Unknown"
        row.append(host_html)
      if table != 'deleted'
        row.append("<td class='blur_table_row_count'>#{number_commas(blur_table['row_count'])}</td>")
        row.append("<td class='blur_table_record_count'>#{number_commas(blur_table['record_count'])}</td>")
      if table == 'active'
        row.append("<td class='blur_table_info'><a class='info' href='#{Routes.schema_blur_table_path(id)}' data-remote='true'>view</a></td>")
      return row
  
  no_data = () ->
    $('.no-tables, .remove-next-update').remove()
    if data.length == 0
      $('.check-all').prop('disabled',true).prop('checked',false)
      $('.bulk-action-button').addClass('suppress-button').each (idx,elm) ->
        $(elm).prop('disabled',true)
      $('.blur_table').remove()
      for tbody in $('tbody')
        num_col = $(tbody).closest('table').find('th').length
        $(tbody).append("<tr class='no-tables'><td/><td colspan='#{num_col}'>No Tables Found</td></tr>")
  
  set_checkbox_state = () ->
    for table in $('table')
      tbody = $(table).find('tbody')
      number_of_tables = $(tbody).children().length
      number_of_checkboxes = $(tbody).find('.bulk-action-checkbox').length
      number_checked = $(tbody).find('.bulk-action-checkbox:checked:not(.check-all)').length
      tab_id = $(tbody).closest('div').attr('id')
      $("a[href=##{tab_id}] .counter").text(number_of_tables)   
      if number_of_tables == 0
        $(table).find('.check-all').prop('disabled',true).prop('checked',false)
        num_col = $(tbody).closest('table').find('th').length
        $(tbody).append("<tr class='no-tables'><td/><td colspan='#{num_col}'>No Tables Found</td></tr>")
      else
        check_all = $(table).find('.check-all').removeAttr('disabled')
        if number_of_tables == number_checked
          check_all.prop('checked',true)
        else
          if number_of_checkboxes == 0
            check_all.prop('disabled',true)
          check_all.prop('checked',false)      
  
  rebuild_table = (data) ->
    $('.no-tables, .remove-next-update').remove()
    if data.length == 0
      no_data()
    else
      currently_checked_rows = get_selected_tables()
      for blur_table in data
        selected_row = $('tr[blur_table_id=' + blur_table.id + ']')
        if selected_row.length
          if selected_row.data('status') != state_lookup[blur_table['status']]
            selected_row.remove()
            new_row_container = $("div#cluster_#{blur_table['cluster_id']}_#{table_lookup[blur_table['status']]} table tbody")
            new_row_container.append(build_table_row(blur_table))
          properties_to_update = ['table_name', 'row_count', 'record_count']
          for property in properties_to_update
            selected_row.find('blur_table_' + property).text(blur_table[property])  
          shard_info = get_host_shard_info(blur_table)

          if blur_table['server']
            selected_row.find('blur_table_hosts_shards a').text(shard_info.hosts + "/" + shard_info.shards)
          else
            selected_row.find('blur_table_hosts_shards a').text("Unknown")
        else
          new_row_container = $("div#cluster_#{blur_table['cluster_id']}_#{table_lookup[blur_table['status']]} table tbody")
          new_row_container.append(build_table_row(blur_table))
      set_checkbox_state()
    
    refresh_timeout = setTimeout('window.reload_table_info()', 10000)

  reload_table_info = () ->
    $.get( "#{Routes.reload_blur_tables_path()}", (data) ->
      rebuild_table(data)).error( (data) ->
          if data.status == 409
            window.location.replace(document.location.origin);
        )
  window.reload_table_info = reload_table_info
  reload_table_info()

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
  $('a.hosts, a.info')
    .live 'ajax:success', (evt, data, status, xhr) ->
      title = $(this).attr('class')
      $(data).hide()
      $().popup 
        title: title.substring(0,1).toUpperCase() + title.substring(1)
        titleClass: 'title'
        body:data
        show:(modal) ->
          modal.children().hide()
          popup_tree = $(modal).children('.modal-body').find('.'+title)
          if popup_tree.size() > 0
            setup_filter_tree popup_tree
          modal.children().show()
  
  #Listener for the check all checkbox
  $('.check-all').live 'change', ->
    checked = $(this).is(':checked')
    $(this).prop('checked',checked)
    boxes = $(this).closest('table').children('tbody').find('.bulk-action-checkbox')
    boxes.each (idx, box) ->
      $(box).prop('checked', checked)
      row_highlight(checked, $(this).parents('.blur_table'))
  
  #listener for bulk action checkboxes
  $('.bulk-action-checkbox').live 'change', ->
    cluster_table = $(this).closest('table')
    if !$(this).hasClass 'check-all'
      cluster_table.find('.check-all').prop('checked',false)
    table_row = $(this).parents('.blur_table')
    if table_row.length == 1
      row_highlight($(this).is(':checked'),table_row)
    disable_action(cluster_table)
    num_checked = cluster_table.find('.bulk-action-checkbox:checked').length
    if num_checked == cluster_table.find('tbody tr .bulk-action-checkbox').length
      cluster_table.find('.check-all').prop('checked',true)

  row_highlight = (should_highlight, table_row) ->
    if should_highlight
      table_row.addClass('highlighted-row')
    else
      table_row.removeClass('highlighted-row')
    
  disable_action = (table) ->
    checked = table.find('.bulk-action-checkbox:checked')
    disabled = checked.length == 0
    actions = table.siblings('.btn')
    actions.prop('disabled',disabled)
    if disabled then actions.addClass('suppress-button') else actions.removeClass('suppress-button')

  pending_change = (cluster,tables,state,action) ->
    for table in tables
      blur_table = $("#cluster_#{cluster}_#{state} .cluster_table").find(".blur_table[blur_table_id='#{table}']")
      colspan = colspan_lookup[state]
      table_name = blur_table.find('.blur_table_name').html()
      blur_table.removeClass('highlighted-row').children().remove()
      blur_table.append("<td colspan='#{colspan}' class='remove-next-update'>#{action} #{table_name}...</td>")
    
    
  #Listener for bulk action button
  $('.btn').live 'click', ->
    action = $(this).attr('blur_bulk_action')
    if !action
      return
    cluster_table = $(this).siblings('table')
    cluster_id = cluster_table.attr('blur_cluster_id')
    blur_tables = cluster_table.children('tbody').children('.blur_table')
    checked_tables = blur_tables.filter('.highlighted-row')
    table_ids = new Array()
    blur_tables.each (index, element) ->
      if $(element).children('td').children('input[type="checkbox"]').is(':checked')
        table_ids.push $(element).attr('blur_table_id')
    if table_ids.length <= 0
      return
    sharedAjaxSettings = 
      beforeSend: ->
        clearTimeout(refresh_timeout)
      success: (data) ->
        if(action == 'forget')
          checked_tables.remove()
        rebuild_table(data)
      data:
        tables: table_ids
        cluster_id: cluster_id
    switch action
      when 'enable'
        btns = 
          "Enable" : 
            class: "primary"
            func: ->
              $.extend(sharedAjaxSettings, { type: 'PUT', url: Routes.enable_selected_blur_tables_path() })
              $.ajax(sharedAjaxSettings)
              pending_change(cluster_id, table_ids,'disabled','Enabling')
              $().closePopup()
          "Cancel" :
            func: ->
              $().closePopup()
        title = "Enable Tables"
        msg = "Are you sure you want to enable these tables?"
      when 'disable'
        btns = 
          "Disable" :
            class: "primary"
            func: ->
              $.extend(sharedAjaxSettings, { type: 'PUT', url: Routes.disable_selected_blur_tables_path() })
              $.ajax(sharedAjaxSettings)
              pending_change(cluster_id, table_ids,'active','Disabling')
              $().closePopup()
          "Cancel" :
            func: ->
              $().closePopup()
        title = "Disable Tables"
        msg = "Are you sure you want to disable these tables?"
      when 'forget'
        btns = 
          "Forget" : 
            class: "primary"
            func: ->
              $.extend(sharedAjaxSettings, { type: 'DELETE', url: Routes.forget_selected_blur_tables_path() })
              $.ajax(sharedAjaxSettings)
              pending_change(cluster_id, table_ids,'deleted','Forgetting')
              $().closePopup()
          "Cancel" :
            func: ->
              $().closePopup()
        title = "Forget Tables"
        msg = "Are you sure you want to forget these tables?"
      when 'delete'
        delete_tables = (delete_index) ->
          $.extend(sharedAjaxSettings, { type: 'DELETE', url: Routes.destroy_selected_blur_tables_path() })
          sharedAjaxSettings.data.delete_index = delete_index
          $.ajax(sharedAjaxSettings)
          pending_change(cluster_id, table_ids,'disabled','Deleting')
        btns = 
          "Delete tables and indicies" :
            class: "danger"
            func: ->
              delete_tables(true)
              $().closePopup()
          "Delete tables only" :
            class: "warning"
            func: ->
              delete_tables(false)
              $().closePopup()
          "Cancel":
            func: ->
              $().closePopup()
        title = "Delete Tables"
        msg = 'Do you want to delete all of the underlying table indicies?'
      else
        return
    $().popup
      title:title
      titleClass: 'title'
      body: msg
      btns: btns
    
