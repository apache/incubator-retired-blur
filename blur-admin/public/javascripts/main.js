/* DO NOT MODIFY. This file was compiled Wed, 18 May 2011 16:13:18 GMT from
 * /home/localadmin/blur/blur-tools/blur-admin/app/coffeescripts/main.coffee
 */

(function() {
  $(document).ready(function() {
    var buildQueryHTML, setupQueryList;
    $('#current-queries-table-select').change(function() {
      var table, url;
      table = $(this).val();
      if (table !== ' ') {
        url = '/queries/running/' + table;
        console.log(url);
        return $.ajax({
          url: url,
          type: 'GET',
          dataType: 'json',
          error: function(jqxhr, msg) {
            return alert(msg);
          },
          success: function(data) {
            setupQueryList(data);
            return console.log(data);
          }
        });
      }
    });
    buildQueryHTML = function(query) {
      var queryString;
      queryString = '<ul>';
      queryString += '<li>Query String:<br/>' + query.query.queryStr + '</li>';
      queryString += '<li>CPU Time:<br/>' + query.cpuTime + '</li>';
      queryString += '<li>Real Time:<br/>' + query.realTime + '</li>';
      return queryString += '</ul>';
    };
    return setupQueryList = function(queries) {
      var completed, running;
      completed = [];
      running = [];
      $.each(queries, function(index, query) {
        if (query.complete === 1) {
          return completed[completed.length] = query;
        } else {
          return running[running.length] = query;
        }
      });
      console.log(running);
      console.log(completed);
      console.log(completed.length);
      $('#current-queries-content').empty();
      $('#current-queries-content').append('<h4>Running Queries:</h4>');
      if (running.length > 0) {
        $.each(running, function(index, query) {
          return $('#current-queries-content').append(buildQueryHTML(query));
        });
      } else {
        $('#current-queries-content').append('<p>No running queries</p>');
      }
      $('#current-queries-content').append('<h4>Completed Queries:</h4>');
      if (completed.length > 0) {
        return $.each(completed, function(index, query) {
          return $('#current-queries-content').append(buildQueryHTML(query));
        });
      } else {
        return $('#current-queries-content').append('<p>No completed queries</p>');
      }
    };
  });
}).call(this);
