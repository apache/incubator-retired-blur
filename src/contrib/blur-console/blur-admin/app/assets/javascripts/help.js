//= require jquery
//= require_self

$(document).ready(function(){
  // toggles help sections on click
  $('.help-section').on('click', function(){
    $(this).children('.help-content').slideToggle('fast')
  });
  // remove the padding at the bottom of the help menu
  $('body:has(#help-window)').css('padding-bottom', '0');
});