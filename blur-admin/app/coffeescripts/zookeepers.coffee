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
        curr_shards = $('#' + table.id).find(".stat-cont")



        if this.zookeeper.status != parseInt( curr_zookeeper.attr('status') )
          curr_zookeeper.addClass('ui-state-error')
        #$.each(this.zookeeper.shards, ->
          #if this.status != parseInt( curr_
        #)
      )
      

      #setTimeout(load_dashboard, 5000)


  load_dashboard()
  