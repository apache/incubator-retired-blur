//= require jquery.dataTables
//= require_self

$.extend( $.fn.dataTableExt.oStdClasses, {
  "sSortAsc": "header headerSortDown",
  "sSortDesc": "header headerSortUp",
  "sSortable": "header"
});

$(document).ready(function() {
  var columnDefinitions = [
    {bSortable : false},
    null,
    null,
    null,
    {asSorting: ['desc']}
  ];

  $('#audits_table > table').dataTable({
      "sDom": "<'row'<'span4'i><'span2'r><'span3'f>>t",
      bPaginate: false,
      bProcessing: true,
      bAutoWidth: false,
      bDeferRender: true,
      aoColumns: columnDefinitions
  }).fnSort([[4, 'desc']]);
});
