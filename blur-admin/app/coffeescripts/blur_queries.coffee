$(document).ready ->

  # Change listener for the table selector
  $('#table-select').live('change', ->
    update_table()
  )
  
  # Displays the full query string on hover
  $('[title]').tooltip({})
