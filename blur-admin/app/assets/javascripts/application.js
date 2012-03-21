//= require jquery
//= require jquery-ui
//= require jquery_ujs
//= require modernizr
//= require placeholder
//= require bootstrap
//= require bootstrap-modal-helper
//= require_self

$(document).ready(function(){
  //Zookeeper context switch
  //reload page with new zookeeper
  $('#zookeeper_id').live('change', function(){
    $(this).closest('form').submit();
  });
	$('#change_current_zookeeper').bind('ajax:success', function() {
		window.location.reload();
	});
        
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
    
  //Fix menus with no zookeeper context
  if (typeof Zookeeper != 'undefined' && Zookeeper.instances) {
    $('#env_link, #tables_link, #queries_link, #search_link').click(function(evt){
      var self = this;
      if (Zookeeper.instances.length == 0) {
        alert('There are no Zookeeper Instances registered yet.  This page will not work until then.');
        return false;
      } else if (Zookeeper.instances.length == 1) {
        return;
      } else {
        var select_box = "<div style='text-align:center'><select id='zookeeper_selector' style='font-size: 20px'><option value=''></option>";
        $.each(Zookeeper.instances, function(){
          select_box += "<option value='" + this.id + "'>" + this.name + "</option>";
        });
        select_box += "</select></div>" ;
        $().popup({
          body: select_box,
          title: 'Select a Zookeeper Instance to use:',
          shown: function(){
            $('#zookeeper_selector').change(function(){
              $.ajax(Routes.make_current_zookeeper_path(), 
              {
                type: 'put',
                data:{ id: $(this).val() },
                success: function(){ 
									window.location = self.href; 
								}
              });
              $().closePopup();
            });
          }
        });
        return false;
      }
    });
  }
});