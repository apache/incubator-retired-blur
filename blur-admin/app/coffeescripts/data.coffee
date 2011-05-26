$(document).ready ->
  #Find list items representing folders and
  #style them accordingly.  Also, turn them
  #into links that can expand/collapse the
  #tree leaf.
  $('li > ul').each((i) ->
    #Find this list's parent list item.
    parent_li = $(this).parent('li')
    parent_li.addClass('expandable')
    #Temporarily remove the list from the
    #parent list item, wrap the remaining
    #text in an anchor, then reattach it.
    sub_ul = $(this).remove()
    parent_li.wrapInner('<a/>').find('a').click(() ->
      #Make the anchor toggle the leaf display.
      sub_ul.toggle()
      parent_li.toggleClass('expandable')
      parent_li.toggleClass('collapsible')
    )
    parent_li.append(sub_ul)
  )

  #Hide all lists except the outermost.
  $('ul ul').hide()

  #Creates an AJAX request to enable or disable a table upon
  #Check or uncheck of 'Enable / Disable' checkbox
  $(".enable").click( ->
    #Determine whether table is to be enabled or disabled
    if $(this).is ':checked'
      action = "enable_table"
    else if $(this).not ':checked'
      action = "disable_table"

    url = "/data/" + action + "/" + $(this).attr('id')
    
    result = $.ajax(
      url: url
      type: 'PUT' )
  )

  #FUNCTION delete a table
  $(".delete-table").click( ->
    confirmation = confirm("Are you sure you want to delete this table?")
    if confirmation
      url = "/data/delete_table/" + $(this).attr('id')
      result = $.ajax(
        url: url
        type: 'DELETE')
  )

