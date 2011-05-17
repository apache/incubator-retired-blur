$(function(){
  
  function setupClickListeners()
  {
    //Click function for the list items
    $('.list-item').unbind('click');
    $('.list-item').click(function(){
      var parent = $(this).parent();
      
      if (parent.attr('id') == 'file-list' && $(this).hasClass('ui-state-focus'))
      {
        //list item in the file list double clicked
        //TODO: code to open a directory from the file list
      }else{
        //change which list-item is focused
        parent.children().removeClass('ui-state-focus');
        $(this).addClass('ui-state-focus');
      
        if(parent.attr('id') == 'file-list')
        {
          //list item is in the file list
          //TODO: code to show file/directory info in the info pane
        }else if(parent.attr('id') == 'tree-list')
        {
          //list item is in the tree list
          
          //open the selected directory in the file list
          var path = $(this).children('input').val();
          path = encodeURIComponent(path);
          var url = '/../../afs/dir/list/' + $('#hd').children('input').val() + '/' + path;
          $.ajax({
            url: url,
            type: 'GET',
            dataType: 'json',
            error: function(jqxhr, msg){
              alert(msg);
            },
            success: function(data)
            {
              //clear out the file list and repopulate with new files/directories
              $('#file-list').empty();
              $.each(data, function(name, path){
                $('#file-list').append('<div class="list-item">' + name + '<input type="hidden" value="' + path + '"/></div>');
              });
              setupClickListeners();
            }
          });
        }
      }
    });
    
    //Click function for the arrow icon of the directory tree
    $('.list-item .ui-icon').unbind('click');
    $('.list-item .ui-icon').click(function(){
      //expanding a dir
      if($(this).hasClass('ui-icon-circle-triangle-e'))
      {
        //icon swap
        $(this).removeClass('ui-icon-circle-triangle-e');
        $(this).addClass('ui-icon-circle-triangle-s');
        
        //TODO: drop down the directory selected
        
        //collapsing a dir
      }else{
        
        //icon swap
        $(this).removeClass('ui-icon-circle-triangle-s');
        $(this).addClass('ui-icon-circle-triangle-e');
        
        //TODO: collapse the directory selected
      }
    
    });
  }
  setupClickListeners();
});