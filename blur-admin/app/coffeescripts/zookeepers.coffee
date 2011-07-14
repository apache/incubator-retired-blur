$(document).ready ->
  makeAJAXRequest = () ->
    url = '/'
    $.ajax(
      url: url
      type: 'GET'
    )
    setTimeout(makeAJAXRequest, 60000)

  setTimeout(makeAJAXRequest, 60000)
