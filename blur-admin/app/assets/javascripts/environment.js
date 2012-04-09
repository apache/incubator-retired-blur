$(document).ready(function(){
  $('.confirm-action').on('click', function(){
    var self = $(this);
    var btns = {
      "Remove": {
        "class": "danger",
        func: function() {
          $.ajax({
            type: 'DELETE',
            url: self.attr('href'),
            success: function(data){
              if (self.attr('data-remove') !== true){
                self.closest('li').remove();
              }
              $().closePopup();
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
    var body = '<div>Are you sure that you want to \'' + self.text() + '\'?</div>'

    $().popup({
      title: "Are you sure?",
      titleClass: 'title',
      body: body,
      btns: btns
    });
    return false;
  });
});