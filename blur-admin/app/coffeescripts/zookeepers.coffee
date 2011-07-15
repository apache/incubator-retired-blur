$(document).ready ->
  load_dashboard = () ->
    $.getJSON '/zookeepers/dashboard', (data) ->
      console.log(data)
  load_dashboard()