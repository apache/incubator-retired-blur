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
              if (self.attr('data-reload') === "true"){
                window.location = window.location.origin;
              } else {
                self.closest('li').remove();
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
  $('i').tooltip();
  var numberOfErrors = $('.error').length
  if (numberOfErrors > 0){
    $('#zookeeper').removeClass('btn-success').addClass('btn-warning');
  }

  $('.more-shard-info').live('click', function(){
    $.ajax({
      type: 'GET',
      url: $(this).attr('href'),
      success: function(data){
        var innerHtml = '<ul class="modal-list">'
        for (var index = 0; index < data.length; index++) {
          var datum = data[index];
          innerHtml += '<li class="';
          if (datum.status === 0){
            innerHtml += 'error"';
          } else {
            innerHtml += 'no-error"';
          }
          innerHtml +='><div class="icon" title="Remove This Shard" data-id="' + datum.id + '"><i class="icon-remove"/></div><div class="info">';
          innerHtml += 'Shard: ' + datum.node_name + ' | Blur Version: ' + datum.blur_version + '| Status: ';
          if (datum.status === 1){
            innerHtml += 'Online';
          } else {
            innerHtml += 'Offline';
          }
          innerHtml += '</div></li>';
        }
        innerHtml += '</ul>';
        $().popup({
          title: "Shards",
          titleClass: 'title',
          body: innerHtml
        });
        $('.icon').tooltip();
      }
    })
    return false;
  });

  $('.icon').live('click', function(){
    var self = $(this);
    var id = self.attr('data-id');
    $.ajax({
      type: 'DELETE',
      url: Routes.destroy_shard_zookeeper_path(CurrentZookeeper, id),
      success: function(data){
        self.closest('li').remove();
        $('.tooltip').remove();
      }
    });
  });
});