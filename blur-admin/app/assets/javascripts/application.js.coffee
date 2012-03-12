#= require jquery
#= require jquery_ujs
#= require jquery-ui
#= require modernizr
#= require bootstrap
#= require bootstrap-modal-helper
#= require_self

$(document).ready ->
  # Zookeeper context switch
  # reload page with new zookeeper
  $('#zookeeper_id').live 'change', ->
    $(this).closest('form').submit()
        
  #fade out flash messages for logging in and out
  $("#flash").delay(5000).fadeOut("slow")
  
  # Initialize Help
  $('#page-help').click ()->
    url = window.location.pathname
    tab
    if url == '/'
      tab = "dashboard"
    else if url.substring(1) == 'zookeeper'
      tab = "environment"
    else if url.substring(1) == 'users'
      tab = "admin"
    else 
      pre_tab = url.substring 1
      tab = if pre_tab.indexOf('/') != -1 then pre_tab.substring 0, pre_tab.indexOf '/' else pre_tab
    help_win = window.open Routes.help_path(tab),"Help Menu","menubar=0,resizable=0,width=500,height=800"
      
  $('.help-section').live 'click', ->
    $(this).children('.help-content').slideToggle('fast')

  $('.dropdown-toggle').dropdown();
    
  # Fix menus with no zookeeper context
  if typeof Zookeeper != 'undefined' && Zookeeper.instances
    $('#env_link, #tables_link, #queries_link, #search_link').click (evt)->
      if Zookeeper.instances.length == 0
        alert 'There are no Zookeeper Instances registered yet.  This page will not work until then.'
        return false
      else if Zookeeper.instances.length == 1
        self = this
        $.ajax Routes.make_current_zookeeper_path(), 
          type: 'put',
          data:
            id: Zookeeper.instances[0].id
          success: () ->
            window.location = self.href
        return false
      else
        self = this
        select_box = "<div style='text-align:center'><select id='zookeeper_selector' style='font-size: 20px'><option value=''></option>"
        $.each Zookeeper.instances, () ->
          select_box += "<option value='#{this.id}'>#{this.name}</option>"
        select_box += "</select></div>"
        $().popup
          body:select_box
          title: 'Select a Zookeeper Instance to use:'
          shown: ()->
            $('#zookeeper_selector').change ()->
              $.ajax Routes.make_current_zookeeper_path(), 
                type: 'put',
                data:
                  id: $(this).val()
                success: () ->
                  window.location = self.href
              $().closePopup()
        return false