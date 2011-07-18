$(document).ready ->
  load_dashboard = () ->
    long_queries_length = $('#zookeepers_wrapper').find("> #old_queries").attr('number')
    if !long_queries_length
      long_queries_length = '0'


    $.getJSON '/zookeepers/dashboard', (data) ->
      console.log(data)
      zookeepers = data.zookeepers
      $.each( zookeepers, ->
        table = $('#zookeepers').find("#" + this.zookeeper.id )[0]
        curr_zookeeper = $('#' + table.id).find("th")
        curr_controllers = $('#' + table.id).find(".stat-cont")
        curr_shards = $('#' + table.id).find(".stat-shard")
        bv_controllers = $('#' + table.id).find(".bv-cont")
        bv_shards = $('#' + table.id).find(".bv-shard")

        s = true
        c = true

        if this.zookeeper.status != parseInt( curr_zookeeper.attr('status') )
          curr_zookeeper.addClass('ui-state-error')

        $.each(this.zookeeper.shards, ->
          if this.status == 1
            s = false
            curr_shards.removeClass('ui-icon-green ui-icon-check')
            curr_shards.find('.ui-state-yellow').show()
          if this.status == 0
            s = false
            curr_shards.removeClass('ui-icon-green ui-icon-check')
            curr_shards.find('.ui-state-error').show()
        )

        $.each(this.zookeeper.controllers, ->
          if this.status == 1
            c = false
            curr_controllers.removeClass('ui-icon-green ui-icon-check')
            curr_controllers.find('.ui-state-yellow').show()
          if this.status == 0
            c = false
            curr_controllers.removeClass('ui-icon-green ui-icon-check')
            curr_controllers.find('.ui-state-error').show()
        )

        if s
          curr_shards.find('.ui-state-yellow').hide()
          curr_shards.find('.ui-state-error').hide()
          curr_shards.addClass('ui-icon-green ui-icon-check')
        if c
          curr_controllers.find('.ui-state-yellow').hide()
          curr_controllers.find('.ui-state-error').hide()
          curr_controllers.addClass('ui-icon-green ui-icon-check')

        $shard_bv = []
        $.each(this.zookeeper.shards, ->
          $shard_bv.push(this.blur_version)
        )
        $.unique($shard_bv)

        if $shard_bv.length > 1
          bv_shards.removeClass('ui-icon-green ui-icon-check')
          bv_shards.find('.ui-state-error').show()

        $controller_bv = []
        $.each(this.zookeeper.controllers, ->
          $controller_bv.push(this.blur_version)
        )
        $.unique($controller_bv)

        if $controller_bv.length > 1
          bv_controllers.removeClass('ui-icon-green ui-icon-check')
          bv_controllers.find('.ui-state-error').show()


      )
      



      setTimeout(load_dashboard, 60000)

  load_dashboard()
  