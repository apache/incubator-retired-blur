#= require jquery.dynatree

$(document).ready ->
  ########### METHODS ###############
  resizeSearch = () ->
    headerHeight = parseInt($('.navbar').css('height'))
    footerHeight = parseInt($('#ft').css('height'))
    resultWrapper = $('#results_wrapper')
    $('#results_wrapper').css('max-height', window.innerHeight - (footerHeight + headerHeight + parseInt(resultWrapper.css('margin-top')) + 30))
  
  hide_all_tabs = () ->
    $('.tab:visible').slideUp 'fast'
    $('.arrow_up').hide()
    $('.arrow_down').show()
  
  # method to initialize the filter tree
  setup_filter_tree = () ->
    $('.column_family_filter').dynatree
      checkbox: true,
      selectMode: 3,
      initAjax: get_filter_ajax()

  get_filter_ajax = () ->
    options =
      url: Routes.filters_zookeeper_searches_path(CurrentZookeeper, $('#blur_table').val()),
      type: 'get'

  # Function to enable or disable submit button based on checkbox status
  toggle_submit = () ->
    if $(".column_family_filter").dynatree("getTree").getSelectedNodes().length > 0 and $('#query_string').val() isnt  ''
      $('#search_submit').removeAttr('disabled')
      if $('#save_name').val() isnt ''
        $('#save_button, #update_button').removeAttr('disabled')
    else
      $('#save_button, #update_button, #search_submit').attr('disabled', 'disabled')

  ########### PAGE ACTIONS ##############
  # Setup the filters onload
  $.ui.dynatree.nodedatadefaults["icon"] = false;
  setup_filter_tree()
  $(window).resize ()->
    if prevHeight != window.innerHeight
      resizeSearch()
    prevHeight = window.innerHeight

  ########### PAGE ELEMENT LISTENERS ##############
  # Reload the filters when the table selector is changed
  $('#blur_table').change ->
    $(".column_family_filter").dynatree("option", "initAjax", get_filter_ajax())
    tree = $(".column_family_filter").dynatree("getTree")
    prevMode = tree.enableUpdate(false)
    tree.reload()
    tree.enableUpdate(prevMode)
      
  # listener that checks if the submit button should be enabled on keystrokes
  $('#query_string, #save_name').live "keydown", (name) ->
    if name.keyCode == 13 && !name.shiftKey
      name.preventDefault()
  $('#query_string, #save_name').live "keyup", (name) ->
    #if it is enter then submit else check to see if we can enable the button
    if name.keyCode == 13 && !name.shiftKey
      name.preventDefault()
      if $('#search_submit').attr 'disabled'
        error_content = '<div style="color:red;font-style:italic; font-weight:bold">Invalid query search.</div>'
        $('#results_container').html(error_content)
        $('#results_wrapper').removeClass('hidden')
      else
        $('#search_form').submit()
    else
      toggle_submit()

  #listener that accordion the filter sections
  $('.header').live 'click', ->
    if $('.tab:visible').length > 0
      if $(this).siblings('.tab:visible').length > 0
        $(this).siblings('.tab:visible').slideUp 'fast'
        $(this).find('.arrow_up').hide()
        $(this).find('.arrow_down').show()
      else
        $('.tab').slideToggle 'fast'
        $('.arrow').toggle()
    else
      $(this).siblings('.body').slideDown 'fast'
      $(this).find('.arrow_down').hide()
      $(this).find('.arrow_up').show()
      
  

  ########### more Functions #############

  fetch_error = (error) ->
    $('#results_container').html "<div class='no-results'>An error has occured: #{error}</div>"
    $('#results_wrapper').addClass('noResults').removeClass('hidden')

  no_results = ->
    $('#results_container').html '<div class="no-results">No results for your search.</div>'
    $('#results_wrapper').addClass('noResults').removeClass('hidden')

  # disable buttons on load
  toggle_submit()
  # set up listeners for ajax to show spinner and disable buttons
  $('#loading-spinner').bind 'ajaxStart', ->
    $(this).removeClass('hidden')
  $('#loading-spinner').bind 'ajaxStop', ->
    $(this).addClass('hidden')
  $('#search_submit, #update_button, #save_button').bind 'ajaxStart', ->
    $(this).attr 'disabled', 'disabled'
  $('#search_submit, #update_button, #save_button').bind 'ajaxStop', ->
    toggle_submit()
  $('body').live 'click', ->
    hide_all_tabs()
  $('.tab:visible, .header').live 'click', (e) ->
    e.stopPropagation()

  populate_form = (data) ->
    $(".column_family_filter").dynatree("getRoot").visit (node) ->
      node.select(false)
    $('#result_count').val(data.fetch)
    $('#offset').val(data.offset)
    $('#query_string').val(data.query)
    $('#save_name').val(data.name)
    $('#super_query').prop('checked',false).prop('disabled',false)
    $('#record_only').prop('checked',false).prop('disabled',false)
    if data.super_query
      $('#super_query').click();
    if data.record_only
      $('#record_only').click();

    #check everything in the tree
    for column in data.column_object
      $(".column_family_filter").dynatree("getTree").selectKey(column)
    $('#search_submit').removeAttr('disabled')
    $('#save_button').removeAttr('disabled')
    $('#update_button').removeAttr('disabled')

  retrieve_search = (id) ->
    $.ajax Routes.load_zookeeper_search_path(CurrentZookeeper, id),
      type: 'POST',
      success: (data) ->
        populate_form(data)

  ########### PAGE AJAX LISTENERS ##############
  # fetch the result of a new search
  $('#search_form').submit ->
    form_data = $(this).serializeArray()
    tree = $('.column_family_filter').dynatree('getTree')
    form_data = form_data.concat(tree.serializeArray())
    $.ajax Routes.fetch_results_zookeeper_searches_path(CurrentZookeeper, $('#blur_table').val()),
      data: form_data,
      type: 'post'
      success: (data, status, xhr) ->
        if data
	        $('#results_container').html data
	        resizeSearch()
	        $('#results_wrapper').removeClass('hidden noResults')
        else
	        no_results()
      error: (xhr, status, error)->
        fetch_error error
    return false

  #ajax listener for the edit action
  $('#edit_icon').live 'click', ->
    retrieve_search($(this).parents('.search_element').attr('id'))

  #ajax listener for the delete action
  $('#delete_icon').live 'click', ->
    parent = $(this).parents('.search_element')
    buttons = 
    	"Delete Query":
    	  class: 'primary'
    	  func: ->
      		$().closePopup();
      		$.ajax Routes.delete_zookeeper_search_path(CurrentZookeeper ,parent.attr("id"), $('#blur_table option:selected').val()),
            type: 'DELETE',
            success: (data) ->
              $('#saved .body .saved').html(data)
    	"Cancel":
    	  func: ->
      		$().closePopup();
    $().popup
      btns:buttons
      title:"Delete this saved query?"
      titleClass:'title'
      body: "This will permanently delete the selected saved query. Do you wish to continue?"

  #ajax listener for the save action
  $('#save_button').live 'click', (evt) ->
    form_data = $('#search_form').serializeArray()
    tree = $('.column_family_filter').dynatree('getTree')
    form_data = form_data.concat(tree.serializeArray())
    $.ajax Routes.save_zookeeper_searches_path(CurrentZookeeper),
      type: 'POST',
      data: form_data,
      success: (data) ->
        if data
        #display the results from the save
          $('#searches').replaceWith(data)

  #ajax listener for the update action
  $('#update_button').live 'click', (evt) ->
    match_found = false
    send_request = false
    search_id = ""
    #if the name in the "name" field matches a search then we can update
    $('.search_element').each (index, value) ->
      if $.trim($(value).children('.search-name').text()) == $.trim($('#save_name').val())
        #if we found another matching item do not send the update request
        if match_found
          return send_request = false
        send_request = true
        match_found = true
        search_id = $(value).attr('id')
    if send_request
      form_data = $('#search_form').serializeArray()
      tree = $('.column_family_filter').dynatree('getTree')
      form_data = form_data.concat(tree.serializeArray())
      $.ajax Routes.zookeeper_search_path(CurrentZookeeper, search_id),
        type: 'PUT',
        data: form_data
    else
      if match_found
        contentBody = "There are multiple saves with the same name."
      else
        contentBody = "There are no saved searches with that name."
      message = "An error occurred while trying to update the saved search: " + contentBody + " To fix this error try changing the name."
      $().popup
        title:"Update Error"
        titleClass:'title'
        body: message
  #listener for the superquery and recordOnly checkboxes
  $('#super_query, #record_only').live 'change',(evt) ->
    sq = $('#super_query');
    ro = $('#record_only');
    if sq[0] == $(this)[0]     
      that = ro
    else
      that = sq
    that.prop('disabled',$(this).is(':checked'))
setTimeout ->
  $('#search_submit').click()
,1000
    
    

  
  
