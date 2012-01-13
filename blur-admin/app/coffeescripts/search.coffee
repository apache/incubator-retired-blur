$(document).ready ->
  ########### METHODS ###############
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
        $('#results_wrapper').show()
      else
        $('#search_form').submit()
    else
      toggle_submit()

  # listener that filters results table when filter checks are changed
  $('.check_filter').live 'click', ->
    name = '.'+$(this).attr('name')
    name_split = name.split("_-sep-_")
    element = name_split[0]
    family_header = '#'+name_split[1]
    family_name = '.family_-sep-_'+name_split[1]
    recordId_name = '.column_-sep-_'+name_split[1]+'_-sep-_recordId'
    curr_col_span = $(family_header).attr('colspan')
    max_col_span = $(family_header).attr('children')

    # hide/show all of the columns if 'All' is checked/unchecked
    if name == ".all"
      num_unchecked = $('#neighborhood').find("> ul > .jstree-unchecked").length
      for family in $('.familysets th')
        if family.id?
          family_class = '.family_-sep-_' + family.id
          if num_unchecked is 0 then $(family_class).removeClass('hidden') else $(family_class).addClass('hidden')

    # hide the clicked filter element if the corresponding column is visible
    else if $(name).is(":visible")
      # hide a column
      if element == ".column"
        if curr_col_span <= 2
          name = family_name
        else
          $(family_header).attr('colspan', curr_col_span-1)
          $(family_name + '_-sep-_empty').attr('colspan', curr_col_span-1)
        $(name).addClass('hidden')
      # hide/show column family
      else
        list_length = $('#'+$(this).attr('name')).find("> ul > .jstree-checked").length + 1
        # show column family if some of it's children are unchecked
        if curr_col_span < max_col_span || curr_col_span < list_length
          $(family_header).attr('colspan', max_col_span)
          $(family_name + '_-sep-_empty').attr('colspan', max_col_span)
          $(name).removeClass('hidden')
        # hide column family otherwise
        else
          $(name).addClass('hidden')

    # show the clicked filter element if the corresponding column is hidden
    else
      # show a column
      if element == ".column"
        # show column when column family is already visible
        if $(family_header).is(":visible")
          if $('#result_table').find('thead > tr > ' + name).length > 0
            $(family_header).attr('colspan', 1 + parseInt(curr_col_span))
            $(family_name + '_-sep-_empty').attr('colspan', 1 + parseInt(curr_col_span))
        # show column and column family when column family is not visible
        else
          $(family_header).attr('colspan', 2)
          $(family_name + '_-sep-_empty').attr('colspan', 2)
          $(family_header).removeClass('hidden')
          $(recordId_name).removeClass('hidden')
          $(family_name + '_-sep-_empty').removeClass('hidden')
      # show a family
      else
        $(family_header).attr('colspan', max_col_span)
        $(family_name + '_-sep-_empty').attr('colspan', max_col_span)
      $(name).removeClass('hidden')

  #listener that accordion the filter sections
  $('.header').live 'click', ->
    $(this).siblings('.body').first().slideToggle 'fast'

  ########### more Functions #############

  fetch_error = (error) ->
    message = "<div>An error has occured: #{error}</div>"
    $('#results_container').html message
    $('#results_wrapper').show()

  no_results = ->
    #hides number of results option if there are no results
    message = '<div>No results for your search.</div>'
    $('#results_container').html message
    $('#results_wrapper').show()

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

  # fetch the result of a persisted search
  fetch_result = (id) ->
    $.ajax Routes.fetch_results_path(id, $('#blur_table option:selected').val()),
      type: 'POST',
      success: (data) ->
        if data
          #shows number of results option if there are results
          #If data is returned properly process it
          $('#results_container').html data
          $('#results_wrapper').show()
        else
          no_results()
      error: (jqXHR, textStatus, errorThrown) ->
        fetch_error errorThrown

  ########### PAGE AJAX LISTENERS ##############
  # fetch the result of a new search
  $('#search_form')
    .live 'ajax:success', (evt, data, status, xhr) ->
      if data
        $('#results_container').html data
        $('#results_wrapper').show()
      else
        #hides number of results option if there are no results
        error_content = '<div>No results for your search.</div>'
        $('#results_container').html(error_content)
        $('#results_wrapper').show()
    .live 'ajax:error', (event, xhr, status, error) ->
      fetch_error error

  #ajax listener for the edit action
  $('#edit_icon').live 'click', ->
    retrieve_search($(this).parents('.search_element').attr('id'))

  #ajax listener for the run action
  $('#run_icon').live 'click', ->
    search = $(this).parents('.search_element').attr 'id'
    retrieve_search search
    fetch_result search

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
      if $(value).children('.search-name').text() == $('#save_name').val()
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
      $().popup
        title:"Update Error"
        titleClass:'title'
        body:"<ul>An error occurred while trying to update the saved search, either:
        <li>There are multiple saves with the same name.</li>
        <li>There are no saved searches with that name.</li>
        To fix this error try changing the name.</ul>"
  #listener for the superquery and recordOnly checkboxes
  $('#super_query, #record_only').live 'change',(evt) ->
    sq = $('#super_query');
    ro = $('#record_only');
    if sq[0] == $(this)[0]     
      that = ro
    else
      that = sq
    that.prop('disabled',$(this).is(':checked'))

  
  
