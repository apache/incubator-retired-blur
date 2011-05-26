$(document).ready ->

  #FUNCTION append - Appends a clone of the first such
  #item passed in to the last such item (in the DOM)
  append = (item) ->
    new_item = $(item).first().clone()
    new_item_number = $(item).length + 1
    new_item.attr('id', new_item_number)
    $(item + ':last').after(new_item)

  #FUNCTION append_to_list - Appends a clone of an item
  #in the list to the end of the list.  The item to be 
  #appended is the first item found in the list using the 
  #passed in selector (item_class).
  append_to_list = (item_class, list) ->
    new_item = list.children().filter(item_class).first().clone()
    list.children().filter(item_class).last().after(new_item)

  #Add Alias Button Functionality
  $('#add_alias').click( ->
    append_to_list('li.alias', $(this).parent())
  )
  #Add Column Family Button Functionality
  $('#add_column_family').click( ->
    append_to_list('li.column_family', $(this).parent())
  )
  #Add Column Button Functionality
  $('#add_column').live('click', ->
    append_to_list('li.column', $(this).siblings().filter('ul'))
  )
