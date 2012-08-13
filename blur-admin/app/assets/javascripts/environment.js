$(document).ready(function(){
  $('.confirm-action').on('click', function(){
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
  
  $('i').tooltip();
  $.ajax({
    type: 'GET',
    url: Routes.zookeeper_path(CurrentZookeeper, {format: 'json'}),
    success: function(data){
      if (data.status == 0)
        $('#zookeeper').removeClass('btn-success').addClass('btn-danger');
      
    }
  })

  $('.more-shard-info').live('click', function(){
    $.ajax({
      type: 'GET',
      url: $(this).attr('href'),
      success: function(data){

        var innerHtml = '<ul class="modal-list no-well">'
        if (data.length <= 0){
          innerHtml = '<div>No shards available</div>'
        }
        else{
           
          //Sort Shards by Name        
          for(var i = 0, j, tmp; i < data.length; ++i){
            tmp = data[i];
            for(j = i - 1; j >= 0 && data[j].node_name > tmp.node_name; --j){
              data[j + 1] = data[j];
            }
            data[j + 1] = tmp;
          } 
                    
          //Find offline shards and place them at the top of the list of shards.
          var offline_shards = new Array(), sorted_data = new Array();
          var offline_index = 0;
          for(i = 0; i < data.length; i++){
            if(data[i].status == 0){
              offline_shards[offline_index] = data[i];
              data[i] = null;
              offline_index++;
            }            
          }
          for(i = 0; i < data.length; i++){
            if(data[i] != null){
              offline_shards[offline_index] = data[i];
              offline_index++;
            }
          }


          //Retrieve and output Shards in modal.
          for (index = 0; index < offline_shards.length; index++) {
            var datum = offline_shards[index];
            innerHtml += '<li class="';
            if (datum.status === 0){
              innerHtml += 'error"';
            } else {
              innerHtml += 'no-error"';
            }
            innerHtml +='><div class="icon" title="Remove This Shard" data-id="' + datum.id + '"><i class="icon-remove-sign"/></div><div class="info">';
            
            innerHtml += 'Shard: ' + datum.node_name + ' | Blur Version: ' + datum.blur_version + ' | Status: ';
            if (datum.status === 1){
              innerHtml += 'Online';
            } else {
              innerHtml += 'Offline';
            }
            innerHtml += '</div></li>';
          }
          innerHtml += '</ul>';
        }
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
        parent = self.closest('ul')
        self.closest('li').remove();
        siblings = parent.find('li')
        $('.tooltip').remove();
        if (siblings.length <=0 ){
          window.location.reload(true);
        }
      }
    });
  });
});
