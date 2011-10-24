$(document).ready ->
  ONLINE='green_box'
  OFFLINE='ui-state-error'
  NA='grey_box'
  
  # Updates all fields on the dashboard
  load_dashboard = () ->
    $.getJSON Routes.dashboard_path(), (data) ->
      # Updates the fields for each zookeeper
      zookeepers = data.zookeepers
      long_queries = data.long_queries
      $.each( data, ->
        zookeeper_table = $('#zookeepers').find("#" + this.id )

        # Updates the header showing the zookeeper status
        current_zookeeper = $('#' + zookeeper_table[0].id).find("th")
        if this.status == 1
          current_zookeeper.removeClass(OFFLINE)
                          .addClass(ONLINE)
                          .find('.zookeeper-status')
                          .html('<div> - Online</div>')
        else
          current_zookeeper.removeClass(ONLINE)
                          .addClass(OFFLINE)
                          .find('.zookeeper-status')
                          .html('<div> - Offline</div>')

        # Updates the warning for long queries
        query_message = '<div></div>'
        if parseInt(this.long_running_queries) > 0
          if parseInt(this.long_running_queries,10) == 1
            query_message = '<div><a href="' + Routes.long_running_queries_path(this.id) + '" class="long_running_queries">1</a> query has been running for more than a minute</div>'
          else
            query_message = '<div><a href="' + Routes.long_running_queries_path(this.id) + '" class="long_running_queries">' + this.long_running_queries + '</a> queries have been running for more than a minute</div>'
        zookeeper_table.find('.warning').html(query_message)

        # Updates the fields for the zookeeper's shards
        status_shards = $('#' + zookeeper_table[0].id).find(".stat-shard")
        bv_shards = $('#' + zookeeper_table[0].id).find(".bv-shard")

        if this.shard_total == 0
          bv_shards.find('.shards-bv')
                  .removeClass(ONLINE)
                  .removeClass(OFFLINE)
                  .addClass(NA)
                  .html('<div>No Shards Available</div>')
        else if parseInt(this.shard_version, 10) == 1
          bv_shards.find('.shards-bv')
                  .removeClass(OFFLINE)
                  .removeClass(NA)
                  .addClass(ONLINE)
                  .html('<div>Consistent Blur Versions</div>')
        else if parseInt(this.shard_version, 10) > 1
          bv_shards.find('.shards-bv')
                  .removeClass(ONLINE)
                  .removeClass(NA)
                  .addClass(OFFLINE)
                  .html('<div>Inconsistent Blur Versions</div>')

        number_shards_online = parseInt(this.shard_total,10) - parseInt(this.shard_offline_node,10)
        if number_shards_online > 0
          status_shards.find('.shards-online')
                      .removeClass(NA)
                      .addClass(ONLINE)
                      .find('> .number')
                      .html('<div>' + number_shards_online + '</div>')
        else
          status_shards.find('.shards-online')
                      .removeClass(ONLINE)
                      .addClass(NA)
                      .find('> .number')
                      .html('<div>0</div>')
        if number_shards_online == 1
          status_shards.find('.shards-online > .word').html('<div>Shard Online</div>')
        else
          status_shards.find('.shards-online > .word').html('<div>Shards Online</div>')

        if parseInt(this.shard_offline_node,10) == 0
          status_shards.find('.shards-offline')
                      .removeClass(OFFLINE)
                      .addClass(NA)
                      .find('> .number')
                      .html('<div>0</div>')
        else
          status_shards.find('.shards-offline')
                      .removeClass(NA)
                      .addClass(OFFLINE)
                      .find('> .number')
                      .html('<div>' + parseInt(this.shard_offline_node,10) + '</div>')
        if parseInt(this.shard_offline_node,10) == 1
          status_shards.find('.shards-offline > .word').html('<div>Shard Offline</div>')
        else
          status_shards.find('.shards-offline > .word').html('<div>Shards Offline</div>')

        # Updates the fields for the zookeeper's controllers
        status_controllers = $('#' + zookeeper_table[0].id).find(".stat-cont")
        bv_controllers = $('#' + zookeeper_table[0].id).find(".bv-cont")

        if this.controller_total == 0
          bv_controllers.find('.controllers-bv')
                        .removeClass(ONLINE)
                        .removeClass(OFFLINE)
                        .addClass(NA)
                        .html('<div>No Controllers Available</div>')
        else if parseInt(this.controller_version, 10) == 1
          bv_controllers.find('.controllers-bv')
                        .removeClass(NA)
                        .removeClass(OFFLINE)
                        .addClass(ONLINE)
                        .html('<div>Consistent Blur Versions</div>')
        else if parseInt(this.controller_version, 10) > 1
          bv_controllers.find('.controllers-bv')
                        .removeClass(NA)
                        .removeClass(ONLINE)
                        .addClass(OFFLINE)
                        .html('<div>Inconsistent Blur Versions</div>')

        number_controllers_online = parseInt(this.controller_total,10) - parseInt(this.controller_offline_node,10)
        if number_controllers_online > 0
          status_controllers.find('.controllers-online')
                            .removeClass(NA)
                            .addClass(ONLINE)
                            .find('> .number')
                            .html('<div>' + number_controllers_online + '</div>')
        else
          status_controllers.find('.controllers-online')
                            .removeClass(ONLINE)
                            .addClass(NA)
                            .find('> .number')
                            .html('<div>0</div>')
        if number_controllers_online == 1
          status_controllers.find('.controllers-online > .word').html('<div>Controller Online</div>')
        else
          status_controllers.find('.controllers-online > .word').html('<div>Controllers Online</div>')

        if parseInt(this.controller_offline_node,10) == 0
          status_controllers.find('.controllers-offline')
                            .removeClass(OFFLINE)
                            .addClass(NA)
                            .find('> .number')
                            .html('<div>0</div>')
        else
          status_controllers.find('.controllers-offline')
                            .removeClass(NA)
                            .addClass(OFFLINE)
                            .find('> .number')
                            .html('<div>' + parseInt(this.controller_offline_node,10) + '</div>')          
        if parseInt(this.controller_offline_node,10) == 1
          status_controllers.find('.controllers-offline > .word').html('<div>Controller Offline</div>')
        else
          status_controllers.find('.controllers-offline > .word').html('<div>Controllers Offline</div>')

        $('#zookeepers_wrapper').show()
      )

    # Sets auto updates to run every 5 secs
    setTimeout(load_dashboard, 5000)

  load_dashboard()

  $('.zookeeper_info').live 'click', ->
    window.location = Routes.show_zookeeper_path($(this).children('table').attr('id'))
  $('a.long_running_queries').live 'click', ->
    url = $(this).attr('href')
    $.get url, (data) ->
      $(data).dialog
        modal: true
        draggable: false
        resizable: false
        width: 'auto'
        title: "Long Running Queries"
        close: (event, ui) ->
          $(this).remove()
        open: (event, ui) ->
          $('#no-queries-row').hide()
    false

