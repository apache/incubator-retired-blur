$(document).ready(function(){
  //Ajax request that serializes your columns and persists them to the DB
  var save_pref = function(){
    var user_id = $('#show_user_wrapper').attr('data-user-id');
    $.ajax(Routes.user_preference_path(user_id, 'column'),
    {
      type: 'PUT',
      data: $('#my-cols').sortable('serialize'),
    });
  };

  //Sortable list of your chosen columns
  $('#my-cols').sortable({
    connectWith: "#actual-trash",
    stop: function(){
      save_pref();
    }
  });

  //Trash can droppable, removes from your chosen preferences
  $('#actual-trash').droppable({
    drop: function(event, ui){
      $(ui.draggable).remove();
      $('.sort#my-cols').sortable('refresh');
      $('.fam#' + $(ui.draggable).attr('id')).toggleClass('my-select');
      save_pref();
    }
  });

  //Click event for selecting a column from all possible columns
  $('.fam').live('click', function(){
    $(this).toggleClass('my-select');
    var clicked = $('#' + $(this).attr('id') + '.sel-fam');

    //if the element isnt in the list of selected columns
    //add the column to your pref list
    if (clicked.length == 0)
    {
      $('#no-saved').hide();
      var app = $(this).clone().removeClass('fam my-select').addClass('sel-fam');
      $('#my-cols').append(app.hide());
      app.fadeIn('slow', function(){ save_pref(); });
    }

    //else the element is already in the list of selected columns
    //remove it from the list of selected columns
    else
    {
      clicked.fadeOut('slow', function(){
        clicked.remove();
        if ($('#my-cols').children().length == 1)
        {
          $('#no-saved').fadeIn('fast');
        }
        save_pref();
      });
    }
  });

  $('#pref-title').on('ajaxStart', function(){
    $(this).removeClass('hidden-spinner');
  });
  $('#pref-title').on('ajaxStop', function(){
    $(this).addClass('hidden-spinner');
  });

  //*******Zookeeper dropdown code********
  //Helper functions
  var checkSelectionStatus = function(){
    if ($('#zookeeper_pref option:selected').val() != 1){
      $('#zookeeper_num').hide();
    } else{
      $('#zookeeper_num').show();
    }
    $('#zookeeper_submit').removeAttr('disabled');
  };
  //Page Listeners
  $('#zookeeper_pref').on('change', function(){
   checkSelectionStatus();
  });
  $('#zookeeper_num').on('change', function(){
    $('#zookeeper_submit').removeAttr('disabled');
  });
  $('#zookeeper_submit').on('click', function(){
    var selected_pref = $('#zookeeper_pref option:selected').val();
    var selected_zk = '';
    if (selected_pref == 1 || 2){
      selected_zk = $('#zookeeper_num option:selected').val();
    }
    $.ajax(Routes.user_preference_path($('#show_user_wrapper').attr('data-user-id'), 'zookeeper'), {
      type: 'PUT',
      data: {
        name: selected_pref,
        value: selected_zk
      }
    });
    $('#zookeeper_submit').attr('disabled', 'disabled');
  });
  //Code for onLoad
  checkSelectionStatus();
  $('#zookeeper_submit').attr('disabled', 'disabled');

});

