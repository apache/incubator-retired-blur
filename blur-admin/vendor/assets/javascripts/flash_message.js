$(document).ready(function(){
  // Wait time to hide messages (ms)
  var wait_time = 8000;
  // Static reference to the flash container
  var flash_container = $("#flash");

  // Methods for manipulating the flash messages
  var flashMessageActions = {
    // Show the flash container
    show: function(element){
      element.css('right', '0px');
    },
    // Hide the flash container
    hide: function(element){
      var width = flash_container.css('width');
      element.css('right', '-' + width);
      setTimeout(function(){
        element.remove();
      }, 3000);
    },
    // Add a flash message to the container
    add: function(message, success){
      var className = success ? 'alert-success' : 'alert-error';
      var messageNode = $('<div class="alert ' + className + '">' + message + '</div>');
      // adjust the height to comp
      var siblings = flash_container.find('div').length * 45;
      messageNode.css('bottom', siblings + 'px');
      flash_container.append(messageNode);
      // show the message (delay is for timing issue)
      setTimeout(function(){
        flashMessageActions.show(messageNode);
      }, 10);
      // hide the message after the wait time
      setTimeout(function(){
        flashMessageActions.hide(messageNode);
      }, wait_time);
    },
    // Remove all of the messages from the container
    empty: flash_container.empty
  }

  // Global method for displaying a notification message to the user
  window.Notification = flashMessageActions.add;

  // list of original messages
  var initialNotices = flash_container.find('div');
  // Timeout to auto hide the original messages
  var hideFlash = setTimeout(function(){
    initialNotices.each(function(){
      flashMessageActions.hide($(this));
    });
  }, wait_time);
  
  // Hide the notice when you click on it
  flash_container.on('click', 'div', function(){
    flashMessageActions.hide($(this));
  })

  // Show the initial messages
  initialNotices.each(function(){
    flashMessageActions.show($(this));
  });
});