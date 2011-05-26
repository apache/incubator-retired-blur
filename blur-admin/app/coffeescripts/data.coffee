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
      action = "enable"
    else if $(this).not ':checked'
      action = "disable"

    url = "/data/" + action + "/" + $(this).attr('id')
    
    $.ajax(
      url: url
      type: 'POST'
    )  
  )

  #FUNCTION delete a table
  #TODO: need this to actually delete a table (thrift call needed)
  $(".delete-table").click( ->
    confirmation = confirm("Are you sure you want to delete this table?")
    if confirmation
      url = "/data/delete/" + $(this).attr('id') 
      $.ajax(
        url: url
        type: 'POST'
      )
  )

