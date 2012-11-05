//= require jquery
//= require jquery-ui
//= require jquery_ujs
//= require modernizr
//= require placeholder
//= require bootstrap
//= require bootstrap-modal-helper
//= require templates
//= require underscore
//= require backbone/backbone
//= require backbone/backbone-relational
//= require backbone/backbone-extension
//= require flash_message
//= require_self

$(document).ready(function(){
  // Global variable for the spinner (makes displaying the spinner simpler)
  window.Spinner = $('<img id="loading-spinner" alt="Loading..." src="/assets/loader.gif"/>')
  // Global display message function

  // Determines the help tab that needs to be opened
  $('#page-help').click(function(){
    var url = window.location.pathname;
    var tab;
    if (url === '/' || url === 'zookeepers') {
      tab = "dashboard";
    } else if (url.substring(1) == 'users') {
      tab = "admin";
    } else if (url.match(/hdfs(?!_)/)){
      tab = "hdfs";
    } else {
      var pieces = url.substring(1).split('/');
      if (pieces.length <= 2) {
        tab = pieces[0];
      } else {
        tab = pieces[2];
      }
    }
    window.open(Routes.help_path(tab), "Help Menu","menubar=0,resizable=0,width=500,height=600");
    return false;
  });

  if (typeof Zookeeper !== 'undefined' && Zookeeper.instances){
    $('#env_link, #tables_link, #queries_link, #search_link').click( function(evt){
      var self = this;
      if (Zookeeper.instances.length === 0){
        alert('There are no Zookeeper Instances registered yet.  This page will not work until then.');
        return false;
      } else if (Zookeeper.instances.length === 1 || CurrentZookeeper !== null){
        return;
      } else {
        var select_box = "<div style='text-align:center'><select id='zookeeper_selector' style='font-size: 20px'><option value=''></option>";
        $.each(Zookeeper.instances, function(){
          select_box += "<option value='" + this.id + "'>" + this.name + "</option>";
        });
        select_box += "</select></div>";
        $().popup({
          body: select_box,
          title: 'Select a Zookeeper Instance to use:',
          shown: function(){
            $('#zookeeper_selector').change(function(){
              window.location = window.location.protocol + '//' + window.location.host + '/zookeepers/' + $(this).val() + ($(self).attr('data-url-extension') || '');
            });
          }
        });
        return false;
      }
    });
  }

  $('#zookeeper_id').change(function(){
    if (window.location.href.match(/(zookeepers\/)\d/)){
      window.location = window.location.href.replace(/(zookeepers\/)\d/, '$1' + $(this).val());
    } else {
      window.location = '/zookeepers/' + + $(this).val();
    }
  });

  $('.dropdown-toggle').dropdown();
});
