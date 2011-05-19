$(document).ready ->
  $('#current-queries-table-select').change ->
    table = $(this).val()
    if table != ' '
      url = '/queries/current/' + table
      console.log(url)
      $.ajax(
        url: url
        type: 'GET'
        dataType: 'json'
        error: (jqxhr, msg) ->
          alert(msg)
        success: (data) ->
          setupQueryList(data)
          console.log(data)
      )
    else setupQueryList()

  getQueryStatus = (query) ->
    if query.complete is 1
      return 'complete'
    else if query.inturrupted is 'true'
      return 'interrupted'
    else if query.running is 'true'
      return 'running'
    else return '???'

  buildQueryRowHTML = (query) ->
    queryString = '<tr>'
    queryString +='<td>' + query.query.queryStr + '</td>'
    queryString +='<td>' + query.cpuTime + '/' + query.realTime + '</td>'
    #TODO: Build the status
    queryString +='<td>' + getQueryStatus(query) + '</td>'
    queryString +='<td>' + query.uuid + '</td>'
    #TODO: Generate the actions
    queryString +='<td>' + 'placeholder'+ '</td>'
    queryString += '</tr>'

  setupQueryList = (queries) ->
    $('#queries-table-header ~ tr').remove()
    if queries and queries.length > 0
      $.each(queries, (index, query) ->
        $('#queries-table').append(buildQueryRowHTML(query))
        )
    else $('#queries-table').append('<tr><td colspan=5>No Current Queries</td></tr>')

  $('#current-queries-table-select').change()
