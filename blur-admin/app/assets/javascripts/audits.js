//= require jquery.dataTables
//= require jquery.timepicker
//= require datatables.fnReloadAjax
//= require_self

$.extend( $.fn.dataTableExt.oStdClasses, {
  "sSortAsc": "header headerSortDown",
  "sSortDesc": "header headerSortUp",
  "sSortable": "header"
});

$(document).ready(function() {
  var adjust_time = function(date){
    date.setHours(date.getHours()+1);
    return date;
  };
  var setup_datepickers = function(){
    var default_timepicker_options = {
      showMinute: false,
      maxDate: adjust_time(new Date),
      hourGrid: 6,
      ampm: true,
    };

    var from_now = new Date();
    var from_hours = urlVars['from']
    from_now.setMinutes(0);



    var to_now = new Date();
    var to_hours = urlVars['to']
    to_now.setMinutes(0);

    var from_input = $('<input class="from-cal" placeholder="from"/>').datetimepicker(default_timepicker_options);
    var to_input = $('<input class="to-cal" placeholder="to"/>').datetimepicker(default_timepicker_options);

    if (from_hours){
      from_now.setHours(from_now.getHours() - from_hours);
      from_input.datepicker('setDate', from_now);
    }

    if (to_hours){
      to_now.setHours(to_now.getHours() - to_hours);
      to_input.datepicker('setDate', to_now);
    }

    $('.row > .span2').prepend('<label>Audit range:</label>', from_input, to_input ,'<button class="btn refresh-button">Refresh</button>');
  };

  var urlVars = function() {
    var vars = {};
    var parts = window.location.href.replace(/[?&]+([^=&]+)=([^&]*)/gi, function(m, key, value) {
      vars[key] = value;
    });
    return vars;
  }();

  var columnDefinitions = [
    {mData: 'action', bSortable : false},
    {mData: 'username', bVisible : false, bSortable : false},
    {mData: 'user'},
    {mData: 'model'},
    {mData: 'mutation'},
    {mData: 'date_audited', asSorting: ['desc', 'asc']}
  ];

  var audit_data_table = $('#audits_table > table').dataTable({
      sDom: "<'row'<'span4'i><'span3'f><'span2'r>>t",
      bPaginate: false,
      bProcessing: true,
      bAutoWidth: false,
      bDeferRender: true,
      aoColumns: columnDefinitions,
      oLanguage: {
        sInfoEmpty: "",
        sInfo: "Displaying _TOTAL_ audits",
        sSearch: "Filter audits:",
        sZeroRecords: "No audits to display",
        sInfoFiltered: "(filtered from _MAX_ total audits)"
      }
  });

  audit_data_table.fnSort([[5, 'desc']]);

  setup_datepickers();
  //Page Listeners
  $('.refresh-button').on('click', function(){
    var now = new Date();
    var from = Math.floor((now - $('.from-cal').datetimepicker('getDate')) / 3600 / 1000);
    var to = Math.floor((now - $('.to-cal').datetimepicker('getDate')) / 3600 / 1000);
    var params = '?from=' + from + '&to=' + to;
    var full_url = window.location.protocol + '//' + window.location.host + window.location.pathname + params;

    if (!Modernizr.history) {
      window.location(full_url);
    } else {
      audit_data_table.fnReloadAjax(Routes.audits_path({format: 'json'}) + params);

      if(location.search.length === 0){
        history.replaceState(null, "Audits | Blur Console", full_url);
      } else if(location.search !== full_url){
        history.pushState(null, "Search | Blur Console", full_url);
      }
    }
  });

  //Overrides default Now button functionality
});



