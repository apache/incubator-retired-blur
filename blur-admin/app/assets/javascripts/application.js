//= require jquery
//= require jquery-ui
//= require jquery_ujs
//= require modernizr
//= require placeholder
//= require bootstrap
//= require bootstrap-modal-helper
//= require_self

$(document).ready(function(){
  //fade out flash messages for logging in and out
  $("#flash").delay(5000).fadeOut("slow");
  
  //Initialize Help
  $('#page-help').click(function(){
    var url = window.location.pathname;
    var tab;
    if (url == '/') {
      tab = "dashboard";
    } else if (url.substring(1) == 'zookeeper') {
      tab = "environment";
    } else if (url.substring(1) == 'users') {
      tab = "admin";
    } else {
      var pre_tab = url.substring(1);
      if (pre_tab.indexOf('/') != -1) {
        tab = pre_tab.substring(0, pre_tab.indexOf('/'));
      } else {
        tab = pre_tab;
      }
    }
    window.open(Routes.help_path(tab), "Help Menu","menubar=0,resizable=0,width=500,height=800");
  });
      
  $('.help-section').live('click', function(){
    $(this).children('.help-content').slideToggle('fast')
  });

  $('.dropdown-toggle').dropdown();
});