$(document).ready ->
  $('#blur_tables').tabs()
  $('.cluster-tabs').tabs()
  
  #converts a number to a string with comma seperation
  number_commas = (number) ->
    if number
      number.toString(10).replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
    else
      'Unknown'
  get_host_shard_info = (blur_table) ->
    server = $.parseJSON(blur_table['server'])
    if server
      keys = Object.keys(server)
      hosts = keys.length
      count = 0
      for key in keys
        count += Object.keys(server[key]).length
      info =
        hosts: hosts
        shards: count
    else
      info =
        hosts: 'Unknown'
        shards: 'Unknown'
    
    
  
  reload_table_info = (cluster, state, shouldRepeat) ->
    
    $.get "#{Routes.reload_blur_tables_path()}?status=#{state}&cluster_id=#{cluster}", (data)->
        selector = $("#cluster_#{cluster}_#{state}")
        cluster_table = selector.children('.cluster_table')
        cluster_table.find('.no-tables').remove()
        $("#inner-tabs-cluster_#{cluster} .#{state}_table_tab a .counter").html(data.length)
        if data.length == 0
          cluster_table.find('.check-all').prop('disabled',true)
          cluster_table.find('.blur_table').remove()
          num_col = cluster_table.find('th').length - 1
          cluster_table.children('tbody').append("<tr class='no-tables'><td/><td colspan='#{num_col}'>No Tables Found</td></tr>")
        else
          cluster_table.find('.check-all').prop('disabled',false)
          for table_hash in data
            blur_table = table_hash['blur_table']
            id = blur_table['id']
            host_info = get_host_shard_info(blur_table)
            existing_table = cluster_table.find(".blur_table[blur_table_id='#{id}']")
            #table exists in table, update row
            if existing_table.length > 0
              existing_table.addClass('updated')
              existing_table.find('.blur_table_name').html(blur_table['table_name'])
              if state != 'deleted'
                existing_table.find('.blur_table_row_count').html(number_commas(blur_table['row_count']))
                existing_table.find('.blur_table_record_count').html(number_commas(blur_table['record_count']))
              if state == 'active'
                if blur_table['server']
                  host_html = "<a class='hosts' href='#{Routes.hosts_blur_table_path(id)}' data-remote='true'>"
                  host_html += "#{host_info['hosts']} / #{host_info['shards']}</a>"
                else
                  host_html = "Unknown"
                existing_table.find('.blur_table_hosts_shards').html(host_html)
                existing_table.find('.blur_table_info').html("<a class='schema' href='#{Routes.schema_blur_table_path(id)}' data-remote='true'>view</a>")
            #table does not exist in table, create new row
            else
              row = $("<tr class='blur_table updated' blur_table_id='#{id}'><td><input class='bulk-action-checkbox' type='checkbox'/></td></tr>")
              row.appendTo(cluster_table.children('tbody'))
              row.append("<td class='blur_table_name'>#{blur_table['table_name']}</td>")
              if state == 'active'
                host_html = "<td class='blur_table_hosts_shards'>"
                if blur_table['server']
                  host_html += "<a class='hosts' href='#{Routes.hosts_blur_table_path(id)}' data-remote='true'>"
                  host_html += "#{host_info['hosts']} / #{host_info['shards']}</a>"
                else
                  host_html += "Unknown"
                row.append(host_html)
              if state != 'deleted'
                row.append("<td class='blur_table_row_count'>#{number_commas(blur_table['row_count'])}</td>")
                row.append("<td class='blur_table_record_count'>#{number_commas(blur_table['record_count'])}</td>")
              if state == 'active'
                row.append("<td class='blur_table_info'><a class='schema' href='#{Routes.schema_blur_table_path(id)}' data-remote='true'>view</a></td>")
        #remove tables that are not updated
        cluster_table.find('.blur_table').not('.updated').remove()  
        cluster_table.find('.blur_table').removeClass('updated')
        disable_action(cluster_table)
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
      $().popup 
        title: title.substring(0,1).toUpperCase() + title.substring(1)
        titleClass: 'title'
        body:data
        show:(modal) ->
          modal.children().hide()
          setup_filter_tree $(modal).children('.modal-body').children()
          modal.children().show()
  
  #Listener for the check all checkbox
  $('.check-all').live 'change', ->
    checked = $(this).is(':checked')
    $(this).prop('checked',checked)
    boxes = $(this).parents('.cluster_table').children('tbody').find('.bulk-action-checkbox')
    boxes.each (idx, box) ->
      $(box).prop('checked', checked)
  
  #listener for bulk action checkboxes
  $('.bulk-action-checkbox').live 'change', ->
    cluster_table = $(this).parents('.cluster_table')
    if !$(this).hasClass 'check-all'
      cluster_table.find('.check-all').prop('checked',false)
    table_row = $(this).parents('.blur_table')
    if table_row.length == 1
      if $(this).is(':checked')
        table_row.addClass('highlighted-row')
      else
        table_row.removeClass('highlighted-row')
    disable_action(cluster_table)
    

   
  disable_action = (table) ->
    checked = table.find('.bulk-action-checkbox:checked')
    disabled = if checked.length == 0 then true else false
    actions = table.siblings('.bulk-action-button, .bulk-action-selector')
    actions.prop('disabled',disabled)
    if disabled then actions.removeClass('primary') else actions.addClass('primary')

  #Listener for bulk action button
  $('.bulk-action-button').live 'click', ->
    action = $(this).siblings('.bulk-action-selector').val()
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
          btns["Delete tables and indicies"] = ->
            delete_tables(true)
            $().closePopup()
          btnClasses["Delete tables and indicies"] = 'danger'
          btns["Delete tables only"] = ->
            delete_tables(false)
            $().closePopup()
          btnClasses["Delete tables only"] = 'danger'
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
