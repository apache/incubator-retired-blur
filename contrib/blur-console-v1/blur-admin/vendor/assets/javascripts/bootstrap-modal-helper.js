/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    params = $.extend({
      title:'',
      body:'',
      btns:{ 'Close': { func: function(){ $('#modal').modal('hide');} } },
      bodyClass:'',
      footerClass:'',
      fade:true,
      backdrop:true,
      keyboard:true,
      onEnter:false
      }, params)
    var title = params['title'];
    var body = params['body'];
    var btns = params['btns'];
    var preShow = params['preShow'];
    var show = params['show'];
    var shown= params['shown'];
    var hide = params['hide'];
    var hidden = params['hidden'];
    var bodyClass = params['bodyClass'];
    var footerClass = params['footerClass'];
    var fade = params['fade'];
    var backdrop = params['backdrop'];
    var keyboard = params['keyboard'];
    var onEnter = params['onEnter'];

    $('#modal').removeClass('fade').unbind('hide').unbind('hidden').modal('hide');
    $('#modal').remove();

    var modal = $("<div id='modal' class='modal'></div>");
    $('body').append(modal);
    if(fade){
      modal.addClass('fade');
    }

    modalHeader = $("<div class='modal-header'><h3>" + title + "</h3></div>");
    modalBody = $("<div class='modal-body'></div>");
    modalFooter = $("<div class='modal-footer'></div>");

    modal.append(modalHeader);
    modal.append(modalBody);
    modalBody.addClass(bodyClass);
    modal.append(modalFooter);
    modalFooter.addClass(footerClass);

    var clone = null;
    if($(this).length == 0){
      modalBody.html(body);
    }else{
      var clone = $(this).first().clone(true);
      $(this).first().replaceWith('<div id="modal-placeholder-div" style="display:none;"></div>');
      modalBody.html(clone);
    }

    for(buttonName in btns){
      buttonProps = btns[buttonName]
      button = $("<button class='btn'>" + buttonName + "</button>")
      if (buttonProps['class']){
        button.addClass('btn-' + buttonProps['class']);
      }
      if (buttonProps['func']){
        button.bind('click', buttonProps['func']);
      } else {
        throw "No functionality for button: " + buttonName;
      }
      modalFooter.prepend(button);
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
    if(onEnter) {
      $('#modal').on('keyup', function(e){
        if (e.which == 13){
          $('.btn-primary').click();
        }
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
      keyboard: keyboard
    });
  }
})( jQuery );
