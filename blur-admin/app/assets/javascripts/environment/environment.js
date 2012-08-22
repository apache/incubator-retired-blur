//= require bootstrap-tooltip
//= require bootstrap-popover
//= require_tree .

$(document).ready(function(){
  new ZookeeperModel().stream(5000);

  $('#bd').on('click', '.confirm-action', function(){
    var self = $(this);
    var btns = {
      "Remove": {
        "class": "danger",
        func: function() {
          $.ajax({
            type: 'DELETE',
            url: self.attr('data-url'),
            success: function(data){
              if (self.attr('data-reload') === "true"){
                window.location = window.location.origin;
              } else {
                self.closest('tr').remove();
                $().closePopup();
              }
            }
          });
        }
      },
      "Cancel": {
        func: function() {
          $().closePopup();
        }
      }
    };
    var body = '<div>Are you sure that you want to ' + self.attr('data-message') + '?</div>'

    $().popup({
      title: "Are you sure?",
      titleClass: 'title',
      body: body,
      btns: btns
    });
    return false;
  });
});
