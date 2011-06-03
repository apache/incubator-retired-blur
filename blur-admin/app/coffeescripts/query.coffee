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
  
  $('#t').change -> $('#filter_columns').load('query/' + $(this).val() + '/filters', setup_filter_tree)

  $('.column_family_filter').click ->
    if $('.jstree-checked')?
      alert "enable"
      $('#search_submit').removeAttr('disabled')
    else
      alert "disable"
      $('#search_submit').attr('disabled', 'disabled')
  
  $('#query_form').bind('ajax:success', (evt, data, status)-> 
    if(data)
      $('#results_section').html(data)
    else
      error_content = '<div style="color:red;font-style:italic; font-weight:bold">No results for your search.</div>'
      $('#results_section').html(error_content)
    true
  )
  $('#query_form').bind('ajax:error', (evt, data, status)-> 
    response = data.responseText
    matches = response.replace(/\n/g,'<br/>').match(/<pre>(.*?)<\/pre>/i)
    
    error_content = '<h3>Error Searching</h3><div style="background:#eee;padding:10px">' + matches[1] + " " + evt.toString() + '</div>'
    #error_content = '<div style="color:red;font-style:italic; font-weight:bold">Please select at least one Column Family to search.</div>'
    $('#results_section').html(error_content)
    true
  )
  
  setup_filter_tree()
  true