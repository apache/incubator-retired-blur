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

  # Function to enable or disable submit button based on checkbox status
  toggle_submit = () ->
    if $('.jstree-checked').length>0 and $('#q').val() isnt  ''
      $(':submit').removeAttr('disabled')
    else
      $(':submit').attr('disabled', 'disabled')
      $('#result_number_section').addClass('hidden')

  $('#t').change -> 
    $('#filter_columns').load('query/' + $(this).val() + '/filters', setup_filter_tree)
    $('#result_number_section').addClass('hidden')
  $('#filter_columns').load('query/' + $('#t').val() + '/filters', setup_filter_tree)

  #functionality for ajax success
  $('#query_form').bind('ajax:success', (evt, data, status)-> 
    if(data)
      #hide the loading image
      $('#loading-spinner').attr("hidden", true)
      #shows number of results option if there are results
      $('#result_number_section').removeClass('hidden')
      #If data is returned properly process it
      $('#results_container').html(data)
      #set the border once the table has content
      #set the proper height based on whether the window or the table are larger
      win_height = $(window).height() - 425
      table_height = $('.result_table').height()
      if win_height < table_height
        $('#results_section').css('height', win_height) 
      else
        $('#results_section').css('height', table_height)
      $('#results_section').css('border', 'solid 1px #AAA')
    else
      #hide the loading image
      $('#loading-spinner').attr("hidden", true)
      #hides number of results option if there are no results
      $('#result_number_section').addClass('hidden')
      error_content = '<div style="color:red;font-style:italic; font-weight:bold">No results for your search.</div>'
      $('#results_container').html(error_content)
    true
  )
  
  $('#search_submit').live('click', -> 
    $('#loading-spinner').removeAttr("hidden")
  )
  
  #Error message associated with ajax errors
  $('#query_form').bind('ajax:error', (evt, data, status)-> 
    response = data.responseText
    matches = response.replace(/\n/g,'<br/>').match(/<pre>(.*?)<\/pre>/i)
    error_content = '<h3>Error Searching</h3><div style="background:#eee;padding:10px">' + matches[1] + " " + evt.toString() + '</div>'
    #hides number of results option if there are no results
    $('#result_number_section').addClass('hidden')
    $('#results_section').html(error_content)
    true
  )

  #On window resize set the proper height based on whether the window or the table are larger
  $(window).resize ->
    win_height = $(window).height() - 425
    table_height = $('.result_table').height()
    if win_height < table_height
      $('#results_section').css('height', win_height) 
    else
      $('#results_section').css('height', table_height)

  #fucntionality for check all
  check_all = () ->
     $('.jstree-unchecked').addClass('jstree-checked')
     $('.jstree-undetermined').addClass('jstree-checked')
     $('.jstree-unchecked').removeClass('jstree-unchecked')
     $('.jstree-checked').removeClass('jstree-undetermined')
     $('.jstree-real-checkbox').attr('checked', 'checked')

  #fucntionality for uncheck all
  uncheck_all = () ->
     $('.jstree-checked').addClass('jstree-unchecked')
     $('.jstree-undetermined').addClass('jstree-unchecked')
     $('.jstree-checked').removeClass('jstree-checked')
     $('.jstree-undetermined').removeClass('jstree-undetermined')
     $('.jstree-real-checkbox').removeAttr('checked')

  #functionality for displaying results in a lightbox
  table_screen = () ->
  	win_height = $(window).height() * .9
  	win_width = $(window).width() * .9
  	table_name = $("#t option:selected").val()
  	table_name = "Results for query on " + table_name
  	$('<div id="full_screen_dialog">' + $('#results_section').html() + '</div>').dialog({height: win_height, width: win_width, modal: true, draggable: false, resizable: false, title: table_name, close: (event, ui) -> $("#full_screen_dialog").remove() })

  #Live Listeners for this document
  #listeners for check all and uncheck all
  $('#checkall').live('click', -> check_all())
  $('#uncheckall').live('click', -> uncheck_all())
  $('#fullscreen').live('click', -> table_screen())
  #Disable submit button when no text in input
  $('#q').live("keyup", -> toggle_submit())
  $('#filter_section').live("click", -> toggle_submit())

  $('.ui-widget-overlay').live("click", -> $("#full_screen_dialog").dialog("close"))

  #submits form when number of requested results changes
  $('#r').change -> 
    $('#query_form').submit()
    $('#loading-spinner').removeAttr("hidden")
