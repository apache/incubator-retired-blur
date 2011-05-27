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
        error: (jqxhr, msg) ->
          showTableError('Problem Contacting Server')
        success: (data) ->
          setupQueryList(data)
          if data.length > 0
            setupGraphs(data)
            firstRun = false
      )
    else setupQueryList()

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
    $.each(queries, (index, query) ->
      realTime[index] = query.realTime #query.real_time
      cpuTime[index] =  query.cpuTime #query.cpu_time
      xValues[index] = index
    )
    graphData =
      realTime: realTime
      cpuTime: cpuTime
      xValues: xValues
    return graphData

  #FUNCTION getQueryStatus
  #takes in a query object and returns its status
  getQueryStatus = (query) ->
    if query.complete is 1 #no change
      return 'complete'
    else if query.inturrupted #no change
      return 'interrupted'
    else if query.running #no change
      return 'running'
    else return '???'

  #FUNCTION buildQueryActions
  #takes in a query and returns a string of HTML buttons for its actions
  buildQueryActions = (query) ->
    actionString = ''
    if query.running #no change
      actionString += '<button class="runtime-cancel-query" blur:query-uuid="'
      actionString += query.uuid + '">Cancel</button>' #no change
    actionString

  #FUNCTION setupCancelListeners
  #sets up the listners for the cancel buttons
  setupCancelListeners = () ->
    $('.runtime-cancel-query').unbind('click').click(() ->
      uuid = $(this).attr('blur:query-uuid')
      table = $('#table-select').val()
      url = '/query/cancel/' + table + '/' + uuid
      $.ajax(
        url: url
        type: 'GET'
        )
    )

  #FUNCTION buildQueryRowHTML
  #takes in a query and returns a string of HTML for its query table row
  buildQueryRowHTML = (query) ->
    queryString = '<tr>'
    queryString +='<td>' + query.query.queryStr + '</td>'#query.query.Strquery.query_string
    queryString +='<td>' + query.cpuime + '/' + query.realTime + '</td>'#query.cpu_time & query.real_time
    queryString +='<td>' + getQueryStatus(query) + '</td>'
    queryString +='<td>' + query.uuid + '</td>'#no change
    queryString +='<td>' + buildQueryActions(query)+ '</td>'
    queryString += '</tr>'

  #FUNCTION setupQueryList
  #sets up the query table with the given queries
  setupQueryList = (queries) ->
    $('#queries-table-header ~ tr').remove()
    if queries and queries.length > 0
      $.each(queries, (index, query) ->
        $('#queries-table').append(buildQueryRowHTML(query))
      )
      setupCancelListeners()
    else showTableError('No Current Queries')

  #FUNCTION showTableError
  #takes in an error message and displays it in the table
  showTableError = (errorMsg) ->
    $('#queries-table-header ~ tr').remove()
    $('#queries-table').append('<tr><td colspan=5 class="error">' + errorMsg + '</td></tr>')

  #change listener for the table selector
  $('#table-select').change ->
    #makeAJAXRequest()

  #initial ajax request on page load
  #makeAJAXRequest()