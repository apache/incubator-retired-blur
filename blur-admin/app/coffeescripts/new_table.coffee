$(document).ready ->

  #FUNCTION append - Appends a clone of the first such
  #item passed in to the last such item (in the DOM)
  append = (item) ->
    new_item = $(item).first().clone()
    $(item + ':last').after(new_item)

  #Add Alias Button Functionality
  $('#add_alias').click( ->
    append('li.alias')
  )
  #Add Column Family Button Functionality
  $('#add_column_family').click( ->
    append('li.column_family')
  )
  #Add Column Button Functionality
  $('#add_column').click( ->
    append('li.column')
  )
