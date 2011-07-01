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
    if $('.jstree-checked').length>0 and $('#q').val() isnt  ''
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
      $('#example-box').hide()
      #set the border once the table has content
    else
      #hides number of results option if there are no results
      error_content = '<div style="color:red;font-style:italic; font-weight:bold">No results for your search.</div>'
      $('#results_container').html(error_content)
      $('#example-box').show()
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
    $('#example-box').show()
    $('#loading-spinner').hide()
    $('#bar_section').css('height', $('#query_section').height() )
    true
  )

  # Fucntionality for check all
  check_all = () ->
    $('.column_family_filter').jstree("check_all")
    $('th').show()
    $('td').show()

  # Fucntionality for uncheck all
  uncheck_all = () ->
    $('.column_family_filter').jstree("uncheck_all")
    $('th').hide()
    $('td').hide()
    $('.rowId').show()

  # Listeners for check all and uncheck all
  $('#checkall').live('click', -> check_all())
  $('#uncheckall').live('click', -> uncheck_all())

  # Live listeners for this page
  $('#filter_section').live("click", -> toggle_submit())
  $('.ui-widget-overlay').live("click", -> $("#full_screen_dialog").dialog("close"))

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
    if $('#query_section').is('.partial-page')
      $('#filter_section').hide()
      $('#query_section').removeClass('partial-page')
      $('#query_section').addClass('full-page')
      $('#arrow').removeClass('ui-icon-triangle-1-w')
      $('#arrow').addClass('ui-icon-triangle-1-e')
      $('#bar_section').addClass('collapsed-bar')
    else
      $('#query_section').removeClass('full-page')
      $('#query_section').addClass('partial-page')
      $('#filter_section').show()
      $('#arrow').removeClass('ui-icon-triangle-1-e')
      $('#arrow').addClass('ui-icon-triangle-1-w')
      $('#bar_section').removeClass('collapsed-bar')
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

  $('#query-help-link').live 'click', ->
    $('#help-dialog').dialog(
      modal:true
      draggable:false
      resizable: false
      title:"Query Help"
      width:500
    )

  # Listener to hide dialog on click
  $('.ui-widget-overlay').live("click", -> $(".ui-dialog-content").dialog("close"))
