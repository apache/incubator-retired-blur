$(document).ready ->

  #Hide master column array element because it is initially not needed
  $('.column_array').hide()

  #Load form-element master copy clones in order to be able to 
  #clone new copies.  If a master copy is not saved before user
  #input is entered the input will be cloned as well.
  alias_clone = $('li.alias').clone()
  column_family_clone = $('li.column_family').clone()
  column_clone = $('li.column').clone()
  column_array_clone = $('li.column_array').clone().show()
  key_value_clone = $('li.key_value').clone()

  #FUNCTION append_to_list - Appends an item to a list.
  append_to_list = (item, list) ->
    list.children().filter('li').last().after(item)

  #Add Alias Button Functionality
  $('#add_alias').click( ->
    append_to_list(alias_clone.clone(), $(this).parent())
  )
  #Add Column Family Button Functionality
  $('#add_column_family').click( ->
    append_to_list(column_family_clone.clone(), $(this).parent())
  )
  #Add Column Button Functionality
  $('.add_column').live('click', ->
    append_to_list(column_clone.clone(), $(this).parent())
  )
  #Add Make Column Array Functionality
  $('.make_column_array').live('click', ->
    $(this).parent().replaceWith(column_array_clone.clone())
  )
  #Add Make Column String Functionality
  $('.make_column').live('click', ->
    $(this).parent().replaceWith(column_clone.clone())
  )
  #Add Key:Value Button Functionality
  $('.add_key_value').live('click', ->
    append_to_list(key_value_clone.clone(), $(this).parent())
  )
