#Function makeCollapsible
makeCollapsible = (list) ->
  plus = 'plus.png';
  minus = 'minus.png';

  list.style.listStyle = 'none'
  list.style.marginLeft = '0'
  list.style.paddingLeft = '0'

  child = list.firstChild;
  while (child != nil)
    child_elem = child.firstChild()
    elems = []
    while (child_elem != null)
      if (child_elem.tagname == 'ul')
        child_elem.style.display = 'none'
        elems.push(child_elem)
      child_elem = child_elem.nextsibling

    button = document.createElement('img')
    button.setAttribute('class', 'closed')
    button.setAttribute('src', 'plus')
    button.onclick = toggle(button, elems)
    child.insertBefore(button,child.firstChild)

  child = child.nextSibling;

#Function toggle
toggle = (curr_button, curr_elems) ->
  plus = 'plus.png';
  minus = 'minus.png';

  return func = () ->
    if (curr_button.getAttribute('class') == 'closed')
      curr_button.setAttribute('class','open')
      curr-button.setAttribute('src','minus')
    else
      curr_button.setAttribute('class','closed')
      curr-button.setAttribute('src','plus')
    for i in curr_elem
      if (i.style.display=='block')
        i.style.display = 'none'
      else
        i.style.display = 'block'

#Function radio buttons?
