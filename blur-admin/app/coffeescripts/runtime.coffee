$(document).ready ->
  #FUNCTION makeAJAXRequest:
  #Runs the AJAX request for current query information
  makeAJAXRequest =() ->
    table = $('#table-select').val()
    if table != ' ' or table != undefined or table != 'undefined'
      url = '/query/current/' + table
      $.ajax(
        url: url
        type: 'GET'
        dataType: 'json'
        error: (jqxhr, msg) ->
          showTableError('Problem Contacting Server')
        success: (data) ->
          setupQueryList(data)
      )
    else setupQueryList()

    setTimeout(makeAJAXRequest, 5000)

  #FUNCTION getQueryStatus
  #takes in a query object and returns its status
  getQueryStatus = (query) ->
    if query.complete is 1
      return 'complete'
    else if query.inturrupted
      return 'interrupted'
    else if query.running
      return 'running'
    else return '???'

  #FUNCTION buildQueryActions
  #takes in a query and returns a string of HTML buttons for its actions
  buildQueryActions = (query) ->
    actionString = ''
    if query.running
      actionString += '<button class="runtime-cancel-query" blur:query-uuid="'
      actionString += query.uuid + '">Cancel</button>'
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
    queryString +='<td>' + query.query.queryStr + '</td>'
    queryString +='<td>' + query.cpuTime + '/' + query.realTime + '</td>'
    queryString +='<td>' + getQueryStatus(query) + '</td>'
    queryString +='<td>' + query.uuid + '</td>'
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
    makeAJAXRequest()

  #initial ajax request on page load
  makeAJAXRequest()
