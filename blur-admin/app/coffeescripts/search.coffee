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

  #method to resize the table dynamically
  sizeTable = (pixels) ->
    if $(window).width() < $('thead').width() || $('thead').width() == null
      $('#results_container').width($(window).width() - pixels)
    else
      $('#results_container').width($('thead').width())

  # Setup the filters onload
  setup_filter_tree()

  # Reload the filters when the table selector is changed
  $('#blur_table').change ->
    $('#filter_columns').hide()
    $('#filter_columns').load('search/' + $(this).val() + '/filters', setup_filter_tree)

  # Function to enable or disable submit button based on checkbox status
  toggle_submit = () ->
    if $('.jstree-checked').length>0 and $('#query_string').val() isnt  ''
      $(':submit').removeAttr('disabled')
    else
      $(':submit').attr('disabled', 'disabled')

  # Show spinner when submit button is clicked
  $('#search_submit').live('click', ->
    $('#loading-spinner').show()

  )

  # Functionality for ajax success
  $('#search_form').bind('ajax:success', (evt, data, status)->

    if(data)
      #shows number of results option if there are results
      #If data is returned properly process it

      $('#results_container').html(data)
      sizeTable(315)

      #set the border once the table has content
    else
      #hides number of results option if there are no results
      error_content = '<div style="color:red;font-style:italic; font-weight:bold">No results for your search.</div>'
      $('#results_container').html(error_content)
      sizeTable(315)
    $('#loading-spinner').hide()
    true
  )
  
  # Error message associated with ajax errors
  $('#search_form').bind('ajax:error', (evt, data, status)->
    response = data.responseText
    matches = response.replace(/\n/g,'<br/>').match(/<pre>(.*?)<\/pre>/i)
    error_content = '<h3>Error Searching</h3><div style="background:#eee;padding:10px">' + matches[1] + " " + evt.toString() + '</div>'
    #hides number of results option if there are no results
    $('#results_container').html(error_content)
    $('#loading-spinner').hide()
    sizeTable(315)
    true
  )

  # Live listeners for this page
  $('#filter_section').live("click", -> toggle_submit())

  # Disable submit button when no text in input
  $('#query_string').live("keypress", (name) ->
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
      sizeTable(70)
      $('#results_container').css('left', 0)
    else
      $('#filter_section').toggle('fast')
      $('#arrow').removeClass('ui-icon-triangle-1-e')
      $('#arrow').addClass('ui-icon-triangle-1-w')
      $('#bar_section').removeClass('collapsed-bar')
      sizeTable(315)
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

  #resizes the table for the window
  $(window).resize( ->
    sizeTable(315)
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

  $('#edit_icon').live('click', ->
    $.ajax('/search/load/'+ $(this).parent().parent().attr('id'), {
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
      }
    )
  )

  $(".saved-label").corner("top")

