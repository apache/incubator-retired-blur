$(document).ready ->
  $('#current-queries-table-select').change ->
    table = $(this).val()
    if table != ' '
      url = '/queries/running/' + table
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
      
  buildQueryHTML = (query) ->
    queryString = '<ul>'
    queryString +='<li>Query String:<br/>' + query.query.queryStr + '</li>'
    queryString +='<li>CPU Time:<br/>' + query.cpuTime + '</li>'
    queryString +='<li>Real Time:<br/>' + query.realTime + '</li>'
    queryString += '</ul>'
    
  setupQueryList = (queries) ->
    completed = []
    running = []
    $.each(queries, (index, query) ->
      if query.complete == 1
        completed[completed.length] = query
      else
        running[running.length] = query
      )
    console.log(running)
    console.log(completed)
    console.log(completed.length)
    $('#current-queries-content').empty()
    #Running Queries
    $('#current-queries-content').append('<h4>Running Queries:</h4>');
    if running.length > 0
      $.each(running, (index, query) ->
        $('#current-queries-content').append(buildQueryHTML(query))
        )
    else
        $('#current-queries-content').append('<p>No running queries</p>')
  
    #Completed Queries  
    $('#current-queries-content').append('<h4>Completed Queries:</h4>');
    if completed.length > 0
      $.each(completed, (index, query) ->
        $('#current-queries-content').append(buildQueryHTML(query))
        )
    else
        $('#current-queries-content').append('<p>No completed queries</p>')

    