//= require jquery.dataTables
//= require_self

$.extend( $.fn.dataTableExt.oStdClasses, {
  "sSortAsc": "header headerSortDown",
  "sSortDesc": "header headerSortUp",
  "sSortable": "header"
});

$(document).ready(function() {
  $('#audits_table > table').dataTable({
      "sDom": "<'row'<'span4'i><'span2'r><'span3'f>>t",
      bPaginate: false,
      bProcessing: true,
      bAutoWidth: false,
      bDeferRender: true
  }).fnSort([[4, 'desc']]);
});
