$(document).ready ->
  ########### METHODS ###############
  resizeSearch = () ->
    headerHeight = parseInt($('#top').css('height'))
    footerHeight = parseInt($('#ft').css('height'))
    resultWrapper = $('#results_wrapper')
    $('#results_wrapper').css('height', window.innerHeight - (footerHeight + headerHeight + parseInt(resultWrapper.css('margin-top')) + 10))
  
  hide_all_tabs = () ->
    $('.tab:visible').slideUp 'fast'
    $('.arrow_up').hide()
    $('.arrow_down').show()
  
  # method to initialize the filter tree
  setup_filter_tree = () ->
    $('.column_family_filter').jstree
      plugins: ["themes", "html_data", "checkbox", "sort", "ui"],
      themes:
        theme: 'apple',
        icons: false,
      checkbox:
        override_ui: true,
        real_checkboxes: true,
        real_checkboxes_names: (n)->
          ['column_data[]', n[0].id]
    .delegate "a", "click", -> toggle_submit()

    $('.column_family_filter').bind "loaded.jstree", ->
      $('#filter_columns').removeClass('hidden')

  # Function to enable or disable submit button based on checkbox status
  toggle_submit = () ->
    if $('.jstree-checked').length > 0 and $('#query_string').val() isnt  ''
      $('#search_submit').removeAttr('disabled')
      if $('#save_name').val() isnt ''
        $('#save_button, #update_button').removeAttr('disabled')
    else
      $('#save_button, #update_button, #search_submit').attr('disabled', 'disabled')

  ########### PAGE ACTIONS ##############
  # Setup the filters onload
  setup_filter_tree()
  $(window).resize ()->
    if prevHeight != window.innerHeight
      resizeSearch()
    prevHeight = window.innerHeight

  ########### PAGE ELEMENT LISTENERS ##############
  # Reload the filters when the table selector is changed
  $('#blur_table').change ->
    $('#filter_columns').addClass('hidden')
    $('#filter_columns').load Routes.search_filters_path($(this).val()), setup_filter_tree
    $('#saved .body').load 'reload/' + $(this).val()
      
  # listener that checks if the submit button should be enabled on keystrokes
  $('#query_string, #save_name').live "keydown", (name) ->
    if name.keyCode == 13 && !name.shiftKey
      name.preventDefault()
  $('#query_string, #save_name').live "keyup", (name) ->
    #if it is enter then submit else check to see if we can enable the button
    if name.keyCode == 13 && !name.shiftKey
      name.preventDefault()
      if $('#search_submit').attr 'disabled'
        error_content = '<div style="color:red;font-style:italic; font-weight:bold">Invalid query seach.</div>'
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
    $('#results_container').html "<div>An error has occured: #{error}</div>"
    $('#results_wrapper').removeClass('hidden')

  no_results = ->
    $('#results_container').html '<div>No results for your search.</div>'
    $('#results_wrapper').removeClass('hidden')

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
    search = data.search
    $('.column_family_filter').jstree('uncheck_all')
    $('#result_count').val(search.fetch)
    $('#offset').val(search.offset)
    $('#query_string').val(search.query)
    $('#save_name').val(search.name)
    $('#super_query').prop('checked',false).prop('disabled',false)
    $('#record_only').prop('checked',false).prop('disabled',false)
    if search.super_query
      $('#super_query').click();
    if search.record_only
      $('#record_only').click();

    #uncheck everything so we only check what we saved
    $('.column_family_filter').jstree('uncheck_all')
    #check everything in the tree
    for column in search.column_object
      $('.column_family_filter').jstree 'check_node', "##{column}"
    $('#search_submit').removeAttr('disabled')
    $('#save_button').removeAttr('disabled')
    $('#update_button').removeAttr('disabled')

  retrieve_search = (id) ->
    $.ajax Routes.search_load_path(id),
      type: 'POST',
      success: (data) ->
        populate_form(data)

  ########### PAGE AJAX LISTENERS ##############
  # fetch the result of a new search
  $('#search_form')
    .live 'ajax:success', (evt, data, status, xhr) ->
      if data
        $('#results_container').html data
        resizeSearch()
        $('#results_wrapper').removeClass('hidden')
      else
        #hides number of results option if there are no results
        error_content = '<div>No results for your search.</div>'
        $('#results_container').html(error_content)
        $('#results_wrapper').removeClass('hidden')
    .live 'ajax:error', (event, xhr, status, error) ->
      fetch_error error

  #ajax listener for the edit action
  $('#edit_icon').live 'click', ->
    retrieve_search($(this).parents('.search_element').attr('id'))

  #ajax listener for the delete action
  $('#delete_icon').live 'click', ->
    parent = $(this).parents('.search_element')
    buttons = 
    	"Delete Query": ->
    		$().closePopup();
    		$.ajax Routes.delete_search_path(parent.attr("id"), $('#blur_table option:selected').val()),
          type: 'DELETE',
          success: (data) ->
            $('#saved .body .saved').html(data)
    	"Cancel": ->
    		$().closePopup();
    $().popup
      btns:buttons
      title:"Delete this saved query?"
      titleClass:'title'
      body: "This will permanently delete the selected saved query. Do you wish to continue?"

  #ajax listener for the save action
  $('#save_button').live 'click', (evt) ->
    $.ajax Routes.search_save_path(),
      type: 'POST',
      data: $('#search_form').serialize(),
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
      $.ajax Routes.update_search_path(search_id),
        type: 'PUT',
        data: $('#search_form').serialize()
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
    
    

  
  
