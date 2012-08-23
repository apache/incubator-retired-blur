//= require bootstrap-tooltip
//= require bootstrap-popover
//= require_tree .

// Confirmation popup for forgetting parts of the ZK
var Confirm_Delete = function(options){
  $().popup({
      title: "Are you sure?",
      titleClass: 'title',
      body: '<div>Are you sure that you want to ' + options.message + '?</div>',
      btns: {
        "Remove": {
          "class": "danger",
          func: function(){
            options.confirmed_action();
            $().closePopup();
          }
        },
        "Cancel": {
          func: function() {
            $().closePopup();
          }
        }
      }
    });
};

$(document).ready(function(){
  // Start streaming the model on 5 sec intervals
  new ZookeeperModel().stream(5000);
});
