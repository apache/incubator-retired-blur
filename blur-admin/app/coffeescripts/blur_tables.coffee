$(document).ready ->
  $('#blur_tables').tabs()
  $('.cluster-tabs').tabs()
  
  state_lookup = 
    0 : 'deleted'
    1 : 'disabled'
    2 : 'disabled'
    3 : 'active'
    4 : 'active'
    5 : 'disabled'     
  
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
    state = state_lookup[blur_table['status']]
    id = blur_table['id']
    host_info = get_host_shard_info(blur_table)
    row = $("<tr class='blur_table' blur_table_id='#{id}'><td><input class='bulk-action-checkbox' type='checkbox'/></td></tr>")
    row.data('status', state_lookup[blur_table['status']])
    row.append("<td class='blur_table_name'>#{blur_table['table_name']}</td>")
    if state == 'active'
      host_html = "<td class='blur_table_hosts_shards'>"
      if blur_table['server']
        host_html += "<a class='hosts' href='#{Routes.hosts_blur_table_path(id)}' data-remote='true'>#{host_info['hosts']} / #{host_info['shards']}</a>"
      else
        host_html += "Unknown"
      row.append(host_html)
    if state != 'deleted'
      row.append("<td class='blur_table_row_count'>#{number_commas(blur_table['row_count'])}</td>")
      row.append("<td class='blur_table_record_count'>#{number_commas(blur_table['record_count'])}</td>")
    if state == 'active'
      row.append("<td class='blur_table_info'><a class='info' href='#{Routes.schema_blur_table_path(id)}' data-remote='true'>view</a></td>")

  reload_table_info = () ->
    $.get( "#{Routes.reload_blur_tables_path()}", (data) ->
      $('.no-tables').remove()
      if data.length == 0
        $('.check-all').prop('disabled',true).prop('checked',false)
        $('.bulk-action-button').addClass('suppress-button').each (idx,elm) ->
          $(elm).prop('disabled',true)
        $('.blur_table').remove()
        for tbody in $('tbody')
          num_col = $(tbody).closest('table').find('th').length
          $(tbody).append("<tr class='no-tables'><td/><td colspan='#{num_col}'>No Tables Found</td></tr>")
      else
        currently_checked_rows = get_selected_tables()
        for table_hash in data
          blur_table = table_hash['blur_table']
          selected_row = $('tr[blur_table_id=' + blur_table.id + ']')
          if selected_row.length
            if selected_row.data('status') != state_lookup[blur_table['status']]
              $("table[blur_table_id=#{state_lookup[blur_table['status']]}] tbody").append selected_row.remove()
            properties_to_update = ['table_name', 'row_count', 'record_count']
            for property in properties_to_update
              selected_row.find('blur_table_' + property).text(blur_table[property])  
            shard_info = get_host_shard_info(blur_table)

            if blur_table['server']
              selected_row.find('blur_table_hosts_shards a').text(shard_info.hosts + "/" + shard_info.shards)
            else
              selected_row.find('blur_table_hosts_shards a').text("Unknown")
          else
            new_row_container = $("#cluster_#{blur_table['cluster_id']}_#{state_lookup[blur_table['status']]} tbody")
            new_row_container.append(build_table_row(blur_table))
        for tbody in $('tbody')
          number_of_tables = $(tbody).children().length
          number_checked = $(tbody).find('.bulk-action-checkbox:checked:not(.check-all)').length
          tab_id = $(tbody).closest('div').attr('id')
          $("a[href=##{tab_id}] .counter").text(number_of_tables)   
          if number_of_tables == 0
            $(tbody).find('.check-all').prop('disabled',true).prop('checked',false)
            num_col = $(tbody).closest('table').find('th').length
            $(tbody).append("<tr class='no-tables'><td/><td colspan='#{num_col}'>No Tables Found</td></tr>")
          else
            check_all = $(tbody).siblings('thead').find('.check-all').removeAttr('disabled')
            if number_of_tables == number_checked
              check_all.prop('checked',true)
            else
              check_all.prop('checked',false)
      
      setTimeout('window.reload_table_info()', 15000)).error( (data) ->
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
    boxes = $(this).parents('.cluster_table').children('tbody').find('.bulk-action-checkbox')
    boxes.each (idx, box) ->
      $(box).prop('checked', checked)
      row_highlight(checked, $(this).parents('.blur_table'))
  
  #listener for bulk action checkboxes
  $('.bulk-action-checkbox').live 'change', ->
    cluster_table = $(this).parents('.cluster_table')
    if !$(this).hasClass 'check-all'
      cluster_table.find('.check-all').prop('checked',false)
    table_row = $(this).parents('.blur_table')
    if table_row.length == 1
      row_highlight($(this).is(':checked'),table_row)
    disable_action(cluster_table)

  row_highlight = (should_highlight, table_row) ->
    if should_highlight
      table_row.addClass('highlighted-row')
    else
      table_row.removeClass('highlighted-row')
    
  disable_action = (table) ->
    checked = table.find('.bulk-action-checkbox:checked')
    disabled = if checked.length == 0 then true else false
    actions = table.siblings('.bulk-action-button')
    actions.prop('disabled',disabled)
    if disabled then actions.addClass('suppress-button') else actions.removeClass('suppress-button')

  pending_change = (cluster,table,state,action) ->
    blur_table = $("#cluster_#{cluster}_#{state} .cluster_table").find(".blur_table[blur_table_id='#{table}']")
    if state == 'active'
      colspan = 6
    else if state == 'disabled'
      colspan = 4
    else if state == 'deleted'
      colspan = 2
    table_name = blur_table.find('.blur_table_name').html()
    blur_table.removeClass('highlighted-row').children().remove()
    blur_table.append("<td colspan='#{colspan}'>#{action} #{table_name}...</td>")
    
    
  #Listener for bulk action button
  $('.bulk-action-button').live 'click', ->
    action = $(this).attr('blur_bulk_action')
    if action
      cluster_table = $(this).siblings('.cluster_table')
      cluster_id = cluster_table.attr('blur_cluster_id')
      blur_tables = cluster_table.children('tbody').children('.blur_table')
      table_ids = new Array()
      blur_tables.each (idx,elm) ->
        if $(elm).children('td').children('input[type="checkbox"]').is(':checked')
          table_ids.push $(elm).attr('blur_table_id')
      if table_ids.length > 0
        btns = new Array()
        btnClasses = new Array()
        title = ''
        msg = ''
        if action == 'enable'
          btns["Enable"] = ->
            for idx, table_id of table_ids
              $.ajax
                url: Routes.blur_table_path(table_id)
                type: 'PUT'
                data:
                  cluster_id: cluster_id
                  enable: true
              pending_change(cluster_id, table_id,'disabled','Enabling')
            $().closePopup()
            
          btnClasses['Enable'] = "primary"
          btns["Cancel"] = ->
            $().closePopup()
          title = "Enable Tables"
          msg = "Are you sure you want to enable these tables?"
        else if action == 'disable'
          btns["Disable"] = ->
            for idx, table_id of table_ids
              $.ajax
                url: Routes.blur_table_path(table_id)
                type: 'PUT'
                data:
                  cluster_id: cluster_id
                  disable: true
              pending_change(cluster_id, table_id,'active','Disabling')
            $().closePopup()
          btnClasses['Disable'] = "primary"
          btns["Cancel"] = ->
            $().closePopup()
          title = "Disable Tables"
          msg = "Are you sure you want to disable these tables?"
        else if action == 'forget'
          btns["Forget"] = ->
            for idx, table_id of table_ids
              $.ajax
                url: Routes.forget_blur_table_path(table_id)
                type: 'DELETE'
                data:
                  cluster_id: cluster_id
              pending_change(cluster_id, table_id,'deleted','Forgetting')
            $().closePopup()
          btnClasses['Forget'] = "primary"
          btns["Cancel"] = ->
            $().closePopup()
          title = "Forget Tables"
          msg = "Are you sure you want to forget these tables?"
        else if action == 'delete'
          delete_tables = (delete_index) ->
            for idx, table_id of table_ids
              $.ajax
                url: Routes.blur_table_path(table_id)
                type: 'DELETE'
                data:
                  cluster_id: cluster_id
                  delete_index: delete_index
              pending_change(cluster_id, table_id,'disabled','Deleting')
          btns["Delete tables and indicies"] = ->
            delete_tables(true)
            $().closePopup()
          btnClasses["Delete tables and indicies"] = 'danger'
          btns["Delete tables only"] = ->
            delete_tables(false)
            $().closePopup()
          btnClasses["Delete tables only"] = 'warning'
          btns["Cancel"] = ->
            $().closePopup()
          title = "Delete Tables"
          msg = 'Do you want to delete all of the underlying table indicies?'
          
          
        $().popup
          title:title
          titleClass: 'title'
          body: msg
          btns: btns
          btnClasses: btnClasses
