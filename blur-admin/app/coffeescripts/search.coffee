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
    $('.column_family_filter').bind "loaded.jstree", ->
      $('#filter_columns').show()

  # Function to enable or disable submit button based on checkbox status
  toggle_submit = () ->
    if $('.jstree-checked').length > 0 and $('#query_string').val() isnt  ''
      $('#search_submit').removeAttr('disabled')
      if $('#save_name').val() isnt ''
        $('#save_button').removeAttr('disabled')
      else
        $('#save_button').attr('disabled', 'disabled')
    else
      $(':submit').attr('disabled', 'disabled')

  ########### PAGE ACTIONS ##############
  # Setup the filters onload
  setup_filter_tree()
  $('[title]').tooltip()

  ########### PAGE ELEMENT LISTENERS ##############
  # Reload the filters when the table selector is changed
  $('#blur_table').change ->
    $('#filter_columns').hide()
    $('#filter_columns').load('search/' + $(this).val() + '/filters', setup_filter_tree)
    $('.body#saved').load('reload/' + $(this).val(), ->
      $('[title]').tooltip()
    )

  # Show spinner when submit button is clicked
  $('#search_submit').live 'click', ->
    $('#loading-spinner').show()

  # listener that checks if the submit button should be enabled on click
  $('#filter_section').live "click", -> toggle_submit()

  # listener that checks if the submit button should be enabled on keystrokes
  $('#query_string, #save_name').live "keypress keydown keyup", (name) ->
    #if it is enter then submit else check to see if we can enable the button
    if name.keyCode == 13 && !name.shiftKey
      name.preventDefault()
      if $(':submit').attr 'disabled'
        error_content = '<div style="color:red;font-style:italic; font-weight:bold">Invalid query seach.</div>'
        $('#results_container').html(error_content)
      else
        $('#search_form').submit()
        $('#loading-spinner').show()
    else
      toggle_submit()

  # listener that Hides/Shows filter section
  $('#bar_section').live 'click', ->
    if !($('#filter_section').is(':hidden'))
      $('#filter_section').toggle('fast')
      $('#arrow').removeClass('ui-icon-triangle-1-w')
      $('#arrow').addClass('ui-icon-triangle-1-e')
      $('#results_wrapper').removeClass('open_filters')
      $('#results_wrapper').addClass('collapsed_filters')
      $('#bar_section').width('2em');
    else
      $('#filter_section').toggle('fast')
      $('#arrow').removeClass('ui-icon-triangle-1-e')
      $('#arrow').addClass('ui-icon-triangle-1-w')
      $('#results_wrapper').addClass('open_filters')
      $('#results_wrapper').removeClass('collapsed_filters')
      $('#bar_section').width('1em');
    $('[title]').tooltip()

  # listener that filters results table when filter checks are changed
  $('.check_filter').live 'click', ->
    name = '.'+$(this).attr('name')
    element = name.split("_")[0]
    family = '#'+name.split("_")[1]
    recordId_name = '.column_'+name.split("_")[1]+'_recordId'
    curr_col_span = $(family).attr('colspan')
    max_col_span = $(family).attr('children')

    if $(name).is(":visible")
      if element == ".column"
        if curr_col_span <= 2
          name = '.family_'+name.split("_")[1]
        else
          $(family).attr('colspan', curr_col_span-1)
        $(name).hide()
      else
        list_length = $('#'+$(this).attr('name')).find("> ul > .jstree-checked").length + 1
        if curr_col_span < max_col_span || curr_col_span < list_length
          $(family).attr('colspan', max_col_span)
          $(name).show() #show whole fam
        else
          $(name).hide()
    else
      if element == ".column"
        if $(family).is(":visible")
          if $('#result_table').find('thead > tr > ' + name).length > 0
            $(family).attr('colspan', 1 + parseInt(curr_col_span))
        else
          $(family).attr('colspan', 2)
          $(family).show()
          $(recordId_name).show()
      else
        $(family).attr('colspan', max_col_span)
      $(name).show()

  #listener that accordions the tabs
  $('.header').live 'click', ->
    $(this).siblings('.body').first().slideToggle 'fast'

  ########### PAGE AJAX LISTENERS ##############
  #ajax listener for the response from the search action
  $('#search_form')
    .live 'ajax:beforeSend', (evt, xhr, settings) ->
      $('#loading-spinner').show()
    .live 'ajax:complete', (evt, xhr, status) ->
      $('#loading-spinner').hide()
    .live 'ajax:success', (evt, data, status, xhr) ->
      if data
        #shows number of results option if there are results
        #if data is returned properly process it
        #if the data is from a save then display the html in the filter section
        if $(data).attr("id") == 'searches'
          $('.body#saved').html(data)
        else
          $('#results_container').html data
      else
        #hides number of results option if there are no results
        error_content = '<div>No results for your search.</div>'
        $('#results_container').html(error_content)
      $('[title]').tooltip()
    .live 'ajax:error', (evt, xhr, status, error) ->
      console.log error

  #ajax listener for the edit action
  $('#edit_icon, #run_icon').live 'click', ->
    $.ajax '/search/load/'+ $(this).parent().attr('id'),
      type: 'POST',
      success: (data) ->
        $('.column_family_filter').jstree('uncheck_all')
        $('#result_count').val(data.saved.search.fetch)
        $('#offset').val(data.saved.search.offset)
        $('#query_string').val(data.saved.search.query)
        $('#super_query').val(data.saved.search.super_query)
        arr = eval(data.saved.search.columns)
        $.each arr, (index, value) ->
          $('.column_family_filter').jstree('check_node', "#" + value)
        $('#search_submit').removeAttr('disabled')

  #ajax listener for the run action
  $('#run_icon').live 'click', ->
    $.ajax '/search/'+ $(this).parent().attr('id') + '/' + $('#blur_table option:selected').val(),
      type: 'POST',
      success: (data) ->
        $('#loading-spinner').hide()
        if data
        #shows number of results option if there are results
        #If data is returned properly process it
          $('#results_container').html data
        else
          #hides number of results option if there are no results
          error_content = '<div>No results for your search.</div>'
          $('#results_container').html error_content
        $('[title]').tooltip()
    $('#loading-spinner').show()

  #ajax listener for the delete action
  $('#delete_icon').live 'click', ->
    parent = $(this)
    $( "#dialog-confirm" ).dialog {
    			resizable: false,
    			modal: true,
    			buttons: {
    				"Delete Query": ->
    				  $( this ).dialog "close"
    				  answer = true
    				  $.ajax '/search/delete/'+ parent.parent().attr("id") + '/' + $('#blur_table option:selected').val(),
                type: 'DELETE',
                success: (data) ->
                  $('.body#saved').html(data)
                  $('#loading-spinner').hide()
              $('#loading-spinner').show()
    				Cancel: ->
    					$( this ).dialog "close"
    			}
    		}
    		
  #ajax listener for the save action
  $('#save_button').live 'click', (evt) ->
    $.ajax '/search/save/',
      type: 'POST',
      data: $('#search_form').serialize(),
      success: (data) ->
        $('#loading-spinner').hide()
        if data
        #display the results from the save
          $('.body#saved').html(data)
        $('[title]').tooltip()
    $('#loading-spinner').show()
    evt.preventDefault()

      

