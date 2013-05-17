$(document).ready(function(){
  $('.table-filter').on('ajax:submit', function(){
    $(this).find('.control-group').addClass('info');
  });
  $('.table-filter').on('ajax:success', function(){
    $(this).find('.control-group').addClass('success');
  });
  $('.table-filter').on('ajax:error', function(){
    $(this).find('.control-group').addClass('error');
  });
});