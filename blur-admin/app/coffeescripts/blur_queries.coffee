$(document).ready ->

  $('#filter_wrapper').accordion ({ 
    collapsible: true
    autoHeight: false
    active: false
  })
  
  # Displays the full query string on hover
  $('[title]').tooltip({})

  # Listener for the table selector
  $('#blur_table_id').live('change', ->
   $('#filter_form').submit() 
  )
