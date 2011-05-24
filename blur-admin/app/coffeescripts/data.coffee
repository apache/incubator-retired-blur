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
