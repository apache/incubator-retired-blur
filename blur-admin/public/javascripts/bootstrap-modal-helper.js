/*
Params:
title: A string of text/html to display in the title.  If not html, will be wrapped in a h2 tag
body: A string of text/html to display in the body.  Ignored if a selector is provided
btns: An array where the key is the name of the button, and the value is the click action. Defaults to a single close button
titleClass: class name to apply to the title
bodyClass: class name to apply to the body
footerClass: class name to apply to the footer
btnClass: class name to apply to each button
btnClasses: An array where the keys corrospond with they keys of btns, and the values are a class to add to the button
preshow: callback that runs right before showing the modal, providing the modal object
show: callback that runs after setting up the modal, providing the modal object
shown: callback that runs after the modal has been made visible (after css transitions)
hide: callback that runs when the modal closes
hidden: callback that runs when the modal has finished closing (after css transitions)
fade: boolean, should the modal animate.  Default true
backdrop: Includes a modal-backdrop element. Set backdrop to "static" if you do not want the modal closed when the backdrop is clicked. Default is 'modal-backdrop'
keyboard: boolean, closes the modal when the escape key is pressed.  defaults to true
*/
(function( $ ){
  $.fn.closePopup = function(){
    $('#modal').modal('hide');
  }
  $.fn.popup = function(params) {
    var defaultBtn = new Array();
    defaultBtn["Close"] = function(){$('#modal').modal('hide');};
    params = $.extend({
      title:'',
      body:'',
      btns:defaultBtn,
      titleClass:'',
      bodyClass:'',
      footerClass:'',
      btnClass:'',
      btnClasses: new Array(),
      fade:true,
      backdrop:'modal-backdrop',
      keyboard:true
      },params)
    var title = params['title'];
    var body = params['body'];
    var btns = params['btns'];
    var preShow = params['preShow'];
    var show = params['show'];
    var shown= params['shown'];
    var hide = params['hide'];
    var hidden = params['hidden'];
    var titleClass = params['titleClass'];
    var bodyClass = params['bodyClass'];
    var footerClass = params['footerClass'];
    var btnClass = params['btnClass'];
    var btnClasses = params['btnClasses'];
    var fade = params['fade'];
    var backdrop = params['backdrop'];
    var keyboard = params['keyboard'];
    
    $('#modal').removeClass('fade').unbind('hide').unbind('hidden').modal('hide');
    $('#modal').remove();
    $('body').append("<div id='modal' class='modal'></div>");
    var modal = $('#modal');
    if(fade){
     modal.addClass('fade'); 
    }
    if(title.charAt(0) == "<"){
      modal.append("<div class='modal-header'>" + title + "</div>")
    }else{
      modal.append("<h2 class='modal-header'>" + title + "</h2>");
    }
    modal.children('.modal-header').addClass(titleClass);
    modal.append("<div class='modal-body'></div>");
    var clone = null;
    if($(this).length == 0){
      modal.children('.modal-body').html(body);
    }else{
      var clone = $(this).first().clone(true);
      $(this).first().replaceWith('<div id="modal-placeholder-div" style="display:none;"></div>');
      modal.children('.modal-body').html(clone);
      
    }
    modal.children('.modal-body').addClass(bodyClass);
    modal.append("<div class='modal-footer'></div>");
    var footer = modal.children('.modal-footer');
    footer.addClass(footerClass);
    for(name in btns){
      footer.prepend("<button class='btn'>" + name + "<button>");
      var btn = footer.children("button").first();
      btn.addClass(btnClass).bind('click', btns[name]);
      if(btnClasses.hasOwnProperty(name)){
        btn.addClass(btnClasses[name]);
      }
    }
    
    if(typeof(preShow) =='function'){
      preShow(modal);
    }
    if(typeof(show) == 'function'){
      modal.bind('show',function(){
        show(modal)
      });
    }
    if(typeof(shown) == 'function'){
      modal.bind('shown',function(){
        shown(modal)
      });
    }
    if(typeof(hide) == 'function'){
      modal.bind('hide',function(){
        hide(modal)
      });
    }
    if(typeof(hidden) == 'function'){
      modal.bind('hidden',function(){
        hidden(modal)
      });
    }
    modal.bind('hidden',function(){
      if(clone != null){
        $('#modal-placeholder-div').replaceWith(clone);
      }
      modal.remove();
    });
    modal.modal({
      backdrop: backdrop,
      keyboard: keyboard,
      show: true
    });
  }
})( jQuery );
