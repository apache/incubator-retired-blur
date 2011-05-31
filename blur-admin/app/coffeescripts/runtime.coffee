$(document).ready ->
  #FUNCTION makeAJAXRequest:
  #Runs the AJAX request for current query information
  firstRun = true
  makeAJAXRequest =() ->
    table = $('#table-select').val()
    if table != ' ' and table != undefined and table != 'undefined'
      url = '/query/current/' + table
      $.ajax(
        url: url
        type: 'GET'
        dataType: 'json'
        #error: (jqxhr, msg) ->
          #showTableError('Problem Contacting Server')
        success: (data) ->
          #setupQueryList(data)
          if data.length > 0
            setupGraphs(data)
            firstRun = false
      )
    #else setupQueryList()

    setTimeout(makeAJAXRequest, 5000)

  #FUNCTION setupGraphs
  #takes in the queries and sets up the graphs
  setupGraphs = (queries) ->
    font ="8px 'Fontin Sans', Fontin-Sans, sans-serif"
    graphData = prepGraphData(queries)
    $('#performance-graph').empty()
    cpuGraph = Raphael('performance-graph')
    cpuGraph.g.txtattr.font = font
    cpuGraph.g.linechart(10,0,190,190,graphData.xValues,graphData.cpuTime,{axis:"0 0 1 1"})

    $('#average-time-graph').empty()
    realGraph = Raphael('average-time-graph')
    realGraph.g.txtattr.font = font
    realGraph.g.linechart(10,0,190,190,graphData.xValues,graphData.realTime,{axis:"0 0 1 1"})

    $('#usage-graph').empty()
    usageGraph = Raphael('usage-graph')
    usageGraph.g.txtattr.font = font
    #TODO actually graph something for usage

  #FUNCTION prepGraphData
  #takes in the queries and returns an array of objects containing the coords for the graphs
  prepGraphData = (queries) ->
    realTime = new Array
    cpuTime = new Array
    xValues = new Array
    num = 0
    $.each(queries, (index, query) ->
      realTime[index] = query.realTime
      cpuTime[index] =  query.cpuTime
      xValues[index] = index
      num++
    )
    graphData =
      realTime: realTime
      cpuTime: cpuTime
      xValues: xValues
    return graphData

  #FUNCTION Filter the query table
  filter_table = (table_name) ->
    if table_name == 'all'
      $('#queries-table tr').show()
    else
      $('#queries-table tr').filter(".#{table_name}").show()
      $('#queries-table tr').not(".#{table_name}").hide()

  #change listener for the table selector
  $('#table-select').change ->
    #makeAJAXRequest()
    filter_table($('#table-select').val())


  #initial ajax request on page load
  makeAJAXRequest()

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
