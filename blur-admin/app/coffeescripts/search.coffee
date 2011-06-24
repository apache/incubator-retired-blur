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
  $('#t').change ->
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
      #set the border once the table has content
    else
      #hides number of results option if there are no results
      error_content = '<div style="color:red;font-style:italic; font-weight:bold">No results for your search.</div>'
      $('#results_container').html(error_content)
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
    $('#bar_section').css('height', $('#query_section').height() )
    true
  )

  # Fucntionality for check all
  check_all = () ->
     $('.jstree-unchecked').addClass('jstree-checked')
     $('.jstree-undetermined').addClass('jstree-checked')
     $('.jstree-unchecked').removeClass('jstree-unchecked')
     $('.jstree-checked').removeClass('jstree-undetermined')
     $('.jstree-real-checkbox').attr('checked', 'checked')
     $('th').show()
     $('td').show()

  # Fucntionality for uncheck all
  uncheck_all = () ->
     $('.jstree-checked').addClass('jstree-unchecked')
     $('.jstree-undetermined').addClass('jstree-unchecked')
     $('.jstree-checked').removeClass('jstree-checked')
     $('.jstree-undetermined').removeClass('jstree-undetermined')
     $('.jstree-real-checkbox').removeAttr('checked')
     $('th').hide()
     $('td').hide()
     $('.rowId').show()

  #Live Listeners for this document
  #listeners for check all and uncheck all
  $('#checkall').live('click', -> check_all())
  $('#uncheckall').live('click', -> uncheck_all())

  #Disable submit button when no text in input
  $('#q').live("keyup", (name) ->
    if name.keyCode == 13 && !name.shiftKey
      if $(':submit').attr('disabled')
        error_content = '<div style="color:red;font-style:italic; font-weight:bold">Invalid query seach.</div>'
        $('#results_container').html(error_content)
        $('#bar_section').css('height', $('#query_section').height() )
      else
        $('#search_form').submit()
        $('#loading-spinner').show()
    else
      toggle_submit()
  )

  $('#filter_section').live("click", -> toggle_submit())

  $('.ui-widget-overlay').live("click", -> $("#full_screen_dialog").dialog("close"))

  $('#bar_section').live('click', ->
    if $('#query_section').is('.partial-page')
      $('#arrow').hide()
      $('#arrow').css('position','static')
      $('#filter_section').hide(1000, ->
        $('#query_section').removeClass('partial-page')
        $('#query_section').addClass('full-page')
        $('#arrow').removeClass('ui-icon-triangle-1-w')
        $('#arrow').addClass('ui-icon-triangle-1-e')
        $('#arrow').css('position','fixed')
        $('#arrow').show()
      )
    else
      $('#query_section').removeClass('full-page')
      $('#query_section').addClass('partial-page')
      $('#arrow').hide()
      $('#arrow').css('position','static')
      $('#filter_section').show('blind', { direction: "horizontal" }, 1000, ->
        $('#arrow').removeClass('ui-icon-triangle-1-e')
        $('#arrow').addClass('ui-icon-triangle-1-w')
        $('#arrow').css('position','fixed')
        $('#arrow').show()
      )
  )

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
