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
  
  $('#query_form').bind('ajax:success', (evt, data, status)-> 
    $('#results_section').html(data)
    true
  )
  $('#query_form').bind('ajax:error', (evt, data, status)-> 
    response = data.responseText
    matches = response.replace(/\n/g,'<br/>').match(/<pre>(.*?)<\/pre>/i)
    
    error_content = '<h2>Error Searching</h2><div style="background:#eee;padding:10px">' + matches[1] + '</div>'
    $('#results_section').html(error_content)
    true
  )
  
  setup_filter_tree()
  true