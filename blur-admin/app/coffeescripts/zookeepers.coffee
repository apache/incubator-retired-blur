$(document).ready ->
  load_dashboard = () ->
    $.getJSON '/zookeepers/dashboard', (data) ->
      console.log(data)
      zookeepers = data.zookeepers
      long_queries = parseInt ( data.long_queries )

      if long_queries < 1
        query_message = '<div></div>'
      else if long_queries == 1
        query_message = '<div>1 query has been running for more than a minute</div>'
      else
        query_message = '<div>' + data.long_queries + ' queries have been running for more than a minute</div>'
      $('.warning').html(query_message)

      $.each( zookeepers, ->
        table = $('#zookeepers').find("#" + this.zookeeper.id )[0]
        curr_zookeeper = $('#' + table.id).find("th")
        curr_controllers = $('#' + table.id).find(".stat-cont")
        curr_shards = $('#' + table.id).find(".stat-shard")
        bv_controllers = $('#' + table.id).find(".bv-cont")
        bv_shards = $('#' + table.id).find(".bv-shard")

        s1 = true
        s0 = true
        c1 = true
        c0 = true

        if this.zookeeper.status != parseInt( curr_zookeeper.attr('status') )
          curr_zookeeper.addClass('ui-state-error')

        $.each(this.zookeeper.shards, ->
          if this.status == 1
            s1 = false
            curr_shards.find('.ui-icon-green').hide()
            curr_shards.find('.ui-state-yellow').fadeIn('slow')
          if this.status == 0
            s0 = false
            curr_shards.find('.ui-icon-green').hide()
            curr_shards.find('.ui-state-error').fadeIn('slow')
        )

        $.each(this.zookeeper.controllers, ->
          if this.status == 1
            c1 = false
            curr_controllers.find('.ui-icon-green').hide()
            curr_controllers.find('.ui-state-yellow').fadeIn('slow')
          if this.status == 0
            c0 = false
            curr_controllers.find('.ui-icon-green').hide()
            curr_controllers.find('.ui-state-error').fadeIn('slow')
        )

        if s1
          curr_shards.find('.ui-state-yellow').fadeOut('slow')
        if s0
          curr_shards.find('.ui-state-error').fadeOut('slow')
        if s1 and s0
          curr_shards.find('.ui-icon-green').delay(700).fadeIn('slow')
        if c1
          curr_controllers.find('.ui-state-yellow').fadeOut('slow')
        if c0
          curr_controllers.find('.ui-state-error').fadeOut('slow')
        if c1 and c0
          curr_controllers.find('.ui-icon-green').delay(700).fadeIn('slow')

        $shard_bv = []
        $.each(this.zookeeper.shards, ->
          $shard_bv.push(this.blur_version)
        )
        $.unique($shard_bv)

        if $shard_bv.length > 1
          bv_shards.find('.ui-icon-green').hide()
          bv_shards.find('.ui-state-error').fadeIn('slow')

        $controller_bv = []
        $.each(this.zookeeper.controllers, ->
          $controller_bv.push(this.blur_version)
        )
        $.unique($controller_bv)

        if $controller_bv.length > 1
          bv_controllers.find('.ui-icon-green').hide()
          bv_controllers.find('.ui-state-error').fadeIn('slow')

        $('#zookeepers_wrapper').show()
      )
    setTimeout(load_dashboard, 60000)

  load_dashboard()
