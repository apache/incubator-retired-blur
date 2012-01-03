(function( $ ){

  $.fn.popup = function(title,msg,btns) {
    $('#modal').remove();
    $('body').append("<div id='modal' class='modal fade'></div>");
    var modal = $('#modal');
    modal.append("<h2 class='modal-header title'>" + title + "</h2>");
    modal.append("<div class='modal-body'>" + msg + "</div>");
    modal.append("<div class='modal-footer'></div>");
    var footer = modal.children('.modal-footer');
    var count = 0;
    var length = Object.keys(btns).length;
    for(name in btns){
      footer.prepend("<button class='btn primary'>" + name + "<button>");
      footer.children("button").first().bind('click', btns[name]);
      if(count != 0 && count == length - 1){
        footer.children("button").first().removeClass('primary');
      }
      count ++;
    }
    modal.modal({
      backdrop: 'modal-backdrop',
      keyboard: true,
      show: true
    });
  }
})( jQuery );
