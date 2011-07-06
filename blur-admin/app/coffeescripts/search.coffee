$(document).ready ->
  # Function to initialize the filter tree
  setup_filter_tree = () ->
    $('.column_family_filter').jstree({
      plugins: ["themes", "html_data", "checkbox", "sort", "ui"],
      themes: {
        theme: 'apple',
        icons: false,
      }
      checkbox: {
        override_ui: true,
        real_checkboxes: true,
        real_checkboxes_names: (n)->
          ['column_data[]', n[0].id]
      }
    })
    $('.column_family_filter').bind("loaded.jstree", ->
      $('#filter_columns').show()
      $('#bar_section').show()
    )

  # Setup the filters onload
  setup_filter_tree()

  # Reload the filters when the table selector is changed
  $('#blur_table').change ->
    $('#filter_columns').hide()
    $('#filter_columns').load('search/' + $(this).val() + '/filters', setup_filter_tree)

  # Function to enable or disable submit button based on checkbox status
  toggle_submit = () ->
    if $('.jstree-checked').length>0 and $('#query_string').val() isnt  ''
      $('#search_submit').removeAttr('disabled')
      if $('#save_name').val() isnt ''
        $('#save_submit').removeAttr('disabled')
      else
        $('#save_submit').attr('disabled', 'disabled')
    else
      $(':submit').attr('disabled', 'disabled')

  # Show spinner when submit button is clicked
  $('#search_submit').live('click', ->
    $('#loading-spinner').show()

  )

  $('#search_form')
    .live 'ajax:beforeSend', (evt, xhr, settings) ->
      $('#loading-spinner').show()
    .live 'ajax:complete', (evt, xhr, status) ->
      $('#loading-spinner').hide()
    .live 'ajax:success', (evt, data, status, xhr) ->
      if(data)
        #shows number of results option if there are results
        #If data is returned properly process it
        if($(data).attr("id") == 'searches')
          $('.saved-list').html(data)
          $('#searches').slideToggle('fast')
        else
          $('#results_container').html(data)

      #set the border once the table has content
      else
        #hides number of results option if there are no results
        error_content = '<div>No results for your search.</div>'
        $('#results_container').html(error_content)
    .live 'ajax:error', (evt, xhr, status, error) ->
      console.log error

  # Live listeners for this page
  $('#filter_section').live("click", -> toggle_submit())

  # Disable submit button when no text in input
  $('#query_string, #save_name').live("keypress keydown keyup", (name) ->
    if name.keyCode == 13 && !name.shiftKey
      name.preventDefault()
      if $(':submit').attr('disabled')
        error_content = '<div style="color:red;font-style:italic; font-weight:bold">Invalid query seach.</div>'
        $('#results_container').html(error_content)
      else
        $('#search_form').submit()
        $('#loading-spinner').show()
    else
      toggle_submit()
  )

  # Hides/Shows filter section
  $('#bar_section').live('click', ->
    if !($('#filter_section').is(':hidden'))
      $('#filter_section').toggle('fast')
      $('#arrow').removeClass('ui-icon-triangle-1-w')
      $('#arrow').addClass('ui-icon-triangle-1-e')
      $('#bar_section').addClass('collapsed-bar')
      $('#results_container').css('left', 0)
    else
      $('#filter_section').toggle('fast')
      $('#arrow').removeClass('ui-icon-triangle-1-e')
      $('#arrow').addClass('ui-icon-triangle-1-w')
      $('#bar_section').removeClass('collapsed-bar')
      $('#results_container').css('left', 245)
  )

  # Filters results table when filter checks are changed
  $('.check_filter').live('click', ->
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
            $(family).attr('colspan', 1+parseInt(curr_col_span))
        else
          $(family).attr('colspan', 2)
          $(family).show()
          $(recordId_name).show()
      else
        $(family).attr('colspan', max_col_span)
      $(name).show()
  )

  #display the hidden saved searches
  $('.saved-label').live('click', ->
     $('#searches').slideToggle('fast')
  )

  #display the hidden advanced options
  $('.advanced-label').live('click', ->
     $('.advanced-choices').slideToggle('fast')
  )

  #hide the search options
  $('.standard-label').live('click', ->
     $('.standard-options').slideToggle('fast')
  )

  #hide the search options
  $('.saving-label').live('click', ->
     $('.search_save').slideToggle('fast')
  )

  $('#edit_icon').live('click', ->
    $.ajax('/search/load/'+ $(this).parent().attr('id'), {
      type: 'POST',
      success: (data) ->
        if data.success == false
          #tooltip it up
          alert "We are all screwed"
        $('.column_family_filter').jstree('uncheck_all')
        $('#result_count').val(data.saved.search.fetch)
        $('#offset').val(data.saved.search.offset)
        $('#query_string').val(data.saved.search.query)
        $('#super_query').val(data.saved.search.super_query)
        arr = eval(data.saved.search.columns)
        $.each(arr, (index, value) ->
          $('.column_family_filter').jstree('check_node', "#" + value)
        )
        $('#search_submit').removeAttr('disabled')
      }
    )
  )

