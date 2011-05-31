$(document).ready ->
  #FUNCTION makeAJAXRequest1:
  #Runs the AJAX request for current query information
  firstRun = true
  makeAJAXRequest1 =() ->
    table = $('#table-select').val()
    if table != ' ' and table != undefined and table != 'undefined'
      url = '/query/cpu/' + table
      $.ajax(
        url: url
        type: 'GET'
        dataType: 'json'
        success: (data) ->
          if data.length > 0
            setupGraph1(data)
            firstRun = false
      )
    setTimeout(makeAJAXRequest1, 5000)

  #FUNCTION makeAJAXRequest2:
  #Runs the AJAX request for current query information
  makeAJAXRequest2 =() ->
    table = $('#table-select').val()
    if table != ' ' and table != undefined and table != 'undefined'
      url = '/query/real/' + table
      $.ajax(
        url: url
        type: 'GET'
        dataType: 'json'
        success: (data) ->
          if data.length > 0
            setupGraph2(data)
            firstRun = false
      )
    setTimeout(makeAJAXRequest2, 5000)

  #FUNCTION setupGraph1
  #takes in the queries and sets up the graphs
  setupGraph1 = (queries) ->
    font ="8px 'Fontin Sans', Fontin-Sans, sans-serif"
    graphData = prepGraphData1(queries)
    $('#performance-graph').empty()
    cpuGraph = Raphael('performance-graph')
    cpuGraph.g.txtattr.font = font
    cpuGraph.g.linechart(10,0,190,190,graphData.xValues,graphData.cpuTime,{axis:"0 0 1 1"})

  #FUNCTION prepGraphData1
  #takes in the queries and returns an array of objects containing the coords for the graphs
  prepGraphData1 = (queries) ->
    cpuTime = new Array
    xValues = new Array
    $.each(queries, (index, query) ->
      if (isNaN(query))
        cpuTime[index] = 0
      else
        cpuTime[index] = query
      xValues[index] = index
    )
    graphData =
      cpuTime: cpuTime
      xValues: xValues
    return graphData

  #FUNCTION setupGraph2
  #takes in the queries and sets up the graphs
  setupGraph2 = (queries) ->
    font ="8px 'Fontin Sans', Fontin-Sans, sans-serif"
    graphData = prepGraphData2(queries)
    $('#average-time-graph').empty()
    realGraph = Raphael('average-time-graph')
    realGraph.g.txtattr.font = font
    realGraph.g.linechart(10,0,190,190,graphData.xValues,graphData.realTime,{axis:"0 0 1 1"})

    $('#usage-graph').empty()
    usageGraph = Raphael('usage-graph')
    usageGraph.g.txtattr.font = font
    #TODO actually graph something for usage

  #FUNCTION prepGraphData2
  #takes in the queries and returns an array of objects containing the coords for the graphs
  prepGraphData2 = (queries) ->
    realTime = new Array
    xValues = new Array
    $.each(queries, (index, query) ->
      if(isNaN(query))
        realTime[index] = 0
      else
        realTime[index] = query
      xValues[index] = index
    )
    graphData =
      realTime: realTime
      xValues: xValues
    return graphData

  #change listener for the table selector
  $('#table-select').change ->
    makeAJAXRequest1()
    makeAJAXRequest2()

  #initial ajax request on page load
  makeAJAXRequest1()
  makeAJAXRequest2()

  #sets up the listners for the cancel buttons (mysql)
  $('.runtime-cancel-query').click(() ->
    uuid = $(this).attr('id')
    table = $('#table-select').val()
    url = '/query/cancel/' + table + '/' + uuid
    $.ajax(
      url: url
      type: 'GET'
      )
    )