$(document).ready ->
  # Updates all fields on the dashboard
  load_dashboard = () ->
    $.getJSON '/zookeepers/dashboard', (data) ->
      console.log(data)

      # Displays a warning message if 1 or more queries have been running for over a minute
      long_queries = parseInt ( data.long_queries )
      if long_queries < 1
        query_message = '<div></div>'
      else if long_queries == 1
        query_message = '<div>1 query has been running for more than a minute</div>'
      else
        query_message = '<div>' + data.long_queries + ' queries have been running for more than a minute</div>'
      $('.warning').html(query_message)

      # Updates the fields for each zookeeper
      zookeepers = data.zookeepers
      $.each( zookeepers, ->
        table = $('#zookeepers').find("#" + this.id )

        # Updates the header showing the zookeeper status
        current_zookeeper = $('#' + table[0].id).find("th")
        if this.status == "1"
          current_zookeeper.removeClass('ui-state-error')
        else
          current_zookeeper.addClass('ui-state-error')

        # Updates the fields for the zookeeper's shards
        curr_shards = $('#' + table[0].id).find(".stat-shard")
        bv_shards = $('#' + table[0].id).find(".bv-shard")
        if this.shard_total == "0"
          curr_shards.find('.ui-icon-green, .ui-state-yellow, .ui-state-error').hide()
          bv_shards.find('.ui-icon-green, .ui-state-yellow, .ui-state-error').hide()
          table.find('.no-shards').fadeIn('slow')
        else
          table.find('.no-shards').hide()
          
          s1 = true
          s0 = true
          
          if this.shard_disabled_node != "0"
            s1 = false
            curr_shards.find('.ui-icon-green').hide()
            curr_shards.find('.ui-state-yellow').fadeIn('slow')
          if this.shard_offline_node != "0"
            s0 = false
            curr_shards.find('.ui-icon-green').hide()
            curr_shards.find('.ui-state-error').fadeIn('slow')

          if s1
            curr_shards.find('.ui-state-yellow').fadeOut('slow')
          if s0
            curr_shards.find('.ui-state-error').fadeOut('slow')
          if s1 and s0
            curr_shards.find('.ui-icon-green').delay(700).fadeIn('slow')

          if parseInt(this.shard_version, 10) > 1
            bv_shards.find('.ui-icon-green').hide()
            bv_shards.find('.ui-state-error').fadeIn('slow')

        # Updates the fields for the zookeeper's shards
        curr_controllers = $('#' + table[0].id).find(".stat-cont")
        bv_controllers = $('#' + table[0].id).find(".bv-cont")
        if this.controller_total == "0"
          curr_controllers.find('.ui-icon-green, .ui-state-yellow, .ui-state-error').hide()
          bv_controllers.find('.ui-icon-green, .ui-state-yellow, .ui-state-error').hide()
          table.find('.no-controllers').fadeIn('slow')
        else
          table.find('.no-controllers').hide()
          c1 = true
          c0 = true
          
          if this.controller_disabled_node != "0"
            c1 = false
            curr_controllers.find('.ui-icon-green').hide()
            curr_controllers.find('.ui-state-yellow').fadeIn('slow')
          if this.controller_offline_node != "0"
            c0 = false
            curr_controllers.find('.ui-icon-green').hide()
            curr_controllers.find('.ui-state-error').fadeIn('slow')

          if c1
            curr_controllers.find('.ui-state-yellow').fadeOut('slow')
          if c0
            curr_controllers.find('.ui-state-error').fadeOut('slow')
          if c1 and c0
            curr_controllers.find('.ui-icon-green').delay(700).fadeIn('slow')

          if parseInt(this.controller_version, 10) > 1
            bv_controllers.find('.ui-icon-green').hide()
            bv_controllers.find('.ui-state-error').fadeIn('slow')

        $('#zookeepers_wrapper').show()
      )

    # Sets auto updates to run every minute
    setTimeout(load_dashboard, 5000)

  load_dashboard()
