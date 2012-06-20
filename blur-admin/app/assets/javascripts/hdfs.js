//= require jquery.contextMenu
//= require jquery.osxFinder
//= require d3/d3
//= require flot/flot
//= require_self

$(document).ready(function() {
  var delete_file, draw_radial_graph, finishUploading, make_dir, navigateUsingPath, paste_buffer,
    perform_action, reload_hdfs, show_dir_props, show_hdfs_props, upload, uploadFailed, in_file = [],
    allSelected = [], columnSelected = [], lastClicked, ctrlHeld = false;

  // Old browser support for history push state
  if (typeof history.pushState === 'undefined') {
    history.pushState = function() {};
  }

  // One time use page variable initialization
  (function() {
    var headerHeight = 0;
    var footerHeight = 0;
    window.onload = function() {
      headerHeight = parseInt($('.navbar').css('height'), 10);
      footerHeight = parseInt($('#ft').css('height'), 15);
      var newHeight = window.innerHeight - (footerHeight + headerHeight) - 20;
      $('#hdfs_wrapper').animate({height: newHeight + 'px'}, 400);
    };
    $(window).resize(function() {
      $('#hdfs_wrapper').css('height', window.innerHeight - (footerHeight + headerHeight) - 15);
    });
  })();

  //Page Widget Setup Methods

  var setup_context_menus = function() {
    $('#hdfs_browser li.hdfs_instance').contextMenu(
      { menu: 'hdfs-root-context-menu' },
      function(action, el, pos) {
        perform_action(action, el);
        return false;
      }
    );
    $('#hdfs_browser li.folder').contextMenu(
      { menu: 'hdfs-dir-context-menu' },
      function(action, el, pos) {
        perform_action(action, el);
        return false;
      }
    );
    $('#hdfs_browser li.file').contextMenu(
      {menu: 'hdfs-file-context-menu'},
      function(action, el, pos) {
        perform_action(action, el);
        return false;
      }
    );
    $('#hdfs-dir-context-menu').disableContextMenuItems('#paste');
    if ($('#hdfs_browser').attr('hdfs_editor') === 'false') {
      $('.contextMenu').disableContextMenuItems('#paste,#mkdir,#cut,#rename,#delete');
    }
  };

  var tree_context_menu = function() {
    return $(["<div class='context_menus'>",
        "<ul id='hdfs-root-context-menu' class='contextMenu'>",
          "<li class='mkdir'><a href='#mkdir'>New Folder</a></li>",
          "<li class='edit'><a href='#upload'>Upload File</a></li>",
          "<li class='props separator'><a href='#props'>Properties</a></li>",
        "</ul>",
        "<ul id='hdfs-dir-context-menu' class='contextMenu'>",
          "<li class='mkdir'><a href='#mkdir'>New Folder</a></li>",
          "<li class='edit'><a href='#upload'>Upload File</a></li>",
          "<li class='rename'><a href='#rename'>Rename</a></li>",
          "<li class='cut'><a href='#cut'>Cut</a></li>",
          "<li class='paste'><a href='#paste'>Paste</a></li>",
          "<li class='delete'><a href='#delete'>Delete</a></li>",
          "<li class='props separator'><a href='#dirprops'>Properties</a></li>",
        "</ul>",
        "<ul id='hdfs-file-context-menu' class='contextMenu'>",
          "<li class='rename'><a href='#rename'>Rename</a></li>",
          "<li class='cut'><a href='#cut'>Cut</a></li>",
          "<li class='delete'><a href='#delete'>Delete</a></li>",
        "</ul>",
      "</div>"].join('\n'));
  };

  var draw_radial_graph = function(width, height, json) {
    var showGraphTooltip = function(graph, tipContent) {
      var tooltip = $('<div class="graphtip" ><div id="tipcontent">' + tipContent + '</div></div>');
      $('.radial-graph').append(tooltip);
      var graphWidth = graph.outerWidth();
      var graphHeight = graph.outerHeight();
      var tipWidth = tooltip.outerWidth();
      var tipHeight = tooltip.outerHeight();
      var drawPositionX = (graphWidth / 2) - (tipWidth / 2);
      var drawPositionY = (graphHeight / 2) - (tipHeight / 2);
      tooltip.css({
        top: drawPositionY + 'px',
        left: drawPositionX + 'px'
      });
      tooltip.fadeIn(400);
    };
    var radius = Math.min(width, height) / 2;
    var color = d3.scale.category20c();
    var selector = ".radial-graph";
    $(selector).empty();
    var vis = d3.select(selector).append("svg").attr("width", width).attr("height", height).append("g").attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");
    var partition = d3.layout.partition().sort(function(a, b) {
      return b.size - a.size;
    }).size([2 * Math.PI, radius * radius]).value(function(d) {
      return d.size;
    });
    var arc = d3.svg.arc().startAngle(function(d) {
      return d.x;
    }).endAngle(function(d) {
      return d.x + d.dx;
    }).innerRadius(function(d) {
      return Math.sqrt(d.y);
    }).outerRadius(function(d) {
      return Math.sqrt(d.y + d.dy);
    });
    var path = vis.data([json]).selectAll("path").data(partition.nodes).enter().append("path").attr("display", function(d) {
      if (d.depth) {
        return null;
      } else {
        return "none";
      }
    }).attr("d", arc).attr("fill-rule", "evenodd").style("stroke", "#fff").style("fill", function(d) {
      return color((d.children ? d : d.parent).name);
    }).attr("title", function(d) {
      return d.name;
    });
    var timeoutShowVar = null;
    $('path').hover(function() {
      var title = $(this).attr('title') || "No path name found!";
      $('.graphtip').remove();
      clearTimeout(timeoutShowVar);
      timeoutShowVar = setTimeout((function() {
        showGraphTooltip($('.radial-graph'), title);
      }), 500);
    }, function() {
      clearTimeout(timeoutShowVar);
      $('.graphtip').remove();
    });
  };

  // HDFS Actions

  var pre_cut_file = function(action, el) {
    if (paste_buffer.multiple) {
      $.each(paste_buffer.multiple, function(index, value) {
        $(value).removeClass('to-cut');
      });
    }
    if (paste_buffer.location) {
      $(paste_buffer.location).removeClass('to-cut');
    }
    paste_buffer.location = el;
    paste_buffer.action = action;
    paste_buffer.multiple = columnSelected;
    $('#hdfs-dir-context-menu').enableContextMenuItems('#paste');
    if (paste_buffer.multiple.length > 0) {
      $.each(paste_buffer.multiple, function(index, value) {
        $(value).addClass('to-cut');
      });
    }
    else {
      $(paste_buffer.location).addClass('to-cut');
    }
  };

  var cut_file = function(file, location) {
    var from_id = file.attr('hdfs_id');
    var from_path = file.attr('hdfs_path');
    var to_id = location.attr('hdfs_id');
    var to_path = location.attr('hdfs_path');
    if (from_id === to_id) {
      $.post(Routes.move_hdfs_path(to_id), {
          'from': from_path,
          'to': to_path
        }, function() {
          $('#hdfs-dir-context-menu').disableContextMenuItems('#paste');
          reload_hdfs();
        }
      );
    }
  };

  var rename = function(el) {
    var id = el.attr('hdfs_id');
    var from_path = el.attr('hdfs_path');
    $('<div id="newName"><input></input></div>').popup({
      title: 'New Name',
      titleClass: 'title',
      shown: function() {
        $('#newName input').focus();
      },
      btns: {
        "Create": {
          "class": "primary",
          func: function() {
            var newName = $('#newName input').val();
            var newFullPath = "" + (from_path.substring(0, from_path.lastIndexOf('/') + 1)) + newName;
            var unique = true;
            $.each(el.siblings(), function(index, value){
              if(newFullPath == $(value).attr('hdfs_path')) unique = false;
            });
            if (!unique){
              $().closePopup();
              errorPopup("Name already in use.");
            }
            else{
              $.ajax(Routes.move_hdfs_path(id), {
                type: 'post',
                data: {
                  from: from_path,
                  to: newFullPath
                },
                success: function() {
                  el.attr('hdfs_path', newFullPath);
                  var link = el.find('a');
                  link.html(newName);
                  var href = link.attr('href');
                  link.attr('href', href.replace(from_path, newFullPath));
                  if (el.hasClass('osxSelected')) {
                    var nextWin = el.parents('.innerWindow').next();
                    var display_href = el.find('a').attr('href');
                    nextWin.load(display_href);
                  } else {
                    el.click();
                  }
                }
              });
              $().closePopup();
            }
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

  var delete_file = function(file) {
    var id = file.attr('hdfs_id');
    var path = file.attr('hdfs_path');
    $.post(Routes.delete_file_hdfs_path(id), {
      'path': path
    }, function() {
      reload_hdfs();
    });
  };

  var delete_additional_files = function(clicked_file) {
    $.each(columnSelected, function(index, value){
      if(!(clicked_file[0] == value))
        delete_file($(value));
    });
  };

  var upload = function(el) {
    var id = el.attr('hdfs_id');
    var path = el.attr('hdfs_path');
    var modal_container = $('<div id="upload_form_modal_container"></div>');
    in_file = [];
    $('.osxSelectable[hdfs_path="' + path + '"][hdfs_id=' + id + ']').click();
    var osxWindow = ( path == '/' ? 1 : path.split('/').length );

    modal_container.load(Routes.upload_form_hdfs_path(id), function(data) {
      $().popup({
        body: data,
        title: 'Upload File',
        titleClass: 'title',
        show: function() {
          $('#fpath-input').val(path);
          $('#hdfs-id-input').val(id);
          $.each( $($('.innerWindow')[osxWindow]).find('a'), function (index, value){
            in_file.push($(value).attr('title'));
          });
          $('input[type=file]').change( function(event) {
            if (in_file.indexOf($('#file-input').val().split('\\').pop()) < 0) {
              $('#upload_file_warning').addClass('hidden');
            }
            else {
              $('#upload_file_warning').removeClass('hidden');
            }
          });
        },
        hide: function() {
          !window.uploading;
        }
      });
    });
  };

  var make_dir = function(el) {
    var id = el.attr('hdfs_id');
    var path = el.attr('hdfs_path');
    in_file = [];
    $('.osxSelectable[hdfs_path="' + path + '"][hdfs_id=' + id + ']').click();
    var osxWindow = ( path == '/' ? 1 : path.split('/').length );

    $('<div id="newFolder"><label>Folder Name:</label><input></input></div>').popup({
      title: 'New Folder',
      titleClass: 'title',
      shown: function() {
        $('#newFolder input').focus();
      },
      btns: {
        "Create": {
          "class": 'primary',
          func: function() {
            var newName = $('#newFolder input').val();
            var unique = true;

            $.each( $($('.innerWindow')[osxWindow]).find('a'), function (index, value) {
              in_file.push($(value).attr('title'));
            });

            $.each(in_file, function(index, value) {
              if(newName == value) unique = false;
            });

            if (!unique) {
              $().closePopup();
              errorPopup("Folder or file with this name already in use.");
            }
            else {
              $.ajax(Routes.mkdir_hdfs_path(id), {
                type: 'post',
                data: {
                  fs_path: path,
                  folder: $('#newFolder input').val()
                },
                success: function() {
                  var display_href, nextWin;
                  if (el.hasClass('osxSelected')) {
                    nextWin = el.parents('.innerWindow').next();
                    display_href = el.find('a').attr('href');
                    nextWin.load(display_href);
                  } else {
                    el.click();
                  }
                }
              });
            $().closePopup();
            }
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

  var show_hdfs_props = function(id, name) {
    var title = "HDFS Information (" + name + ")";
    $('.hdfs_instance[hdfs_id=' + id + ']').click();
    $.get(Routes.info_hdfs_path(id), function(data) {
      $(data).popup({
        title: title,
        titleClass: 'title',
        show: function() {
          $.get(Routes.structure_hdfs_path(id), {
            'fs_path': '/'
          }, function(data) {
            draw_radial_graph(520, 400, data);
          });
          $('#modal').css({
            'width': '1120px',
            'margin-left': '-560px'
          });
          $('.modal-footer').css({
            'width': '1090px'
          });
        }
      });
    });
  };

  var show_dir_props = function(id, path) {
    var title = "Properties for " + path;
    $('.osxSelectable[hdfs_path="' + path + '"][hdfs_id=' + id + ']').click();
    $.get(Routes.folder_info_hdfs_path(id), {
      'fs_path': path
    }, function(data) {
      $(data).popup({
        titleClass: 'title',
        title: title,
        show: function() {
          $.get(Routes.slow_folder_info_hdfs_path(id), {
            'fs_path': path
          }, function(data) {
            $('#file_count').html(data.file_count);
            $('#folder_count').html(data.folder_count);
            $('#file_size').html(data.file_size);
          });
          $.get(Routes.structure_hdfs_path(id), {
            'fs_path': path
          }, function(data) {
            draw_radial_graph(520, 400, data);
          });
          $('#modal').css({
            'width': '1120px',
            'margin-left': '-560px'
          });
          $('.modal-footer').css({
            'width': '1090px'
          });
        }
      });
    });
  };

  var perform_action = function(action, el) {
    switch (action) {
      case "delete":
        if (columnSelected.length > 0) {
          if (confirm("Are you sure you wish to delete these files? This action can not be undone.")) {
            delete_additional_files(el);
            delete_file(el);
          }
        }
        else if (confirm("Are you sure you wish to delete " + el.attr('hdfs_path') + "? This action can not be undone.")) {
          delete_file(el);
        }
        break;
      case "cut":
        pre_cut_file(action, el);
        break;
      case "paste":
        if (paste_buffer.action && paste_buffer.action === "cut") {
          if (paste_buffer.multiple.length > 0){
            $.each(paste_buffer.multiple, function(index, value){
              cut_file($(value), el);
            });
          }
          else {
            cut_file(paste_buffer.location, el);
          }
        }
        break;
      case "props":
        show_hdfs_props(el.attr('hdfs_id'), el.attr('hdfs_name'));
        break;
      case "dirprops":
        show_dir_props(el.attr('hdfs_id'), el.attr('hdfs_path'));
        break;
      case "mkdir":
        make_dir(el);
        break;
      case "rename":
        rename(el);
        break;
      case "upload":
        upload(el);
        break;
    }
  };

  //Upload Methods
  // Upload methods on the window so returned JS can call them
  window.finishUploading = function(path) {
    $("li[hdfs_path='" + path + "']").click();
    $().closePopup();
    window.uploading = false;
    reload_hdfs();
  };

  window.uploadFailed = function(error) {
    $('#upload-file').html(error);
    window.uploading = false;
  };

  $('#upload-form').live('submit', function() {
    window.uploading = true;
    $('#upload-file #status').html('<h2>Uploading...</h2>');
    $('#upload-file #upload-button').attr('disabled', 'disabled');
  });

  reload_hdfs = function() {
    $('.osxSelected').removeClass('osxSelected');
    navigateUsingPath();
  };

  /*
    #Methods for HTML History manipulation
  */
  navigateUsingPath = function() {
    var pathPieces = window.location.pathname.split('/').filter(function(member) {
      return member !== '';
    }).slice(1);
    var hdfsId = pathPieces.shift();
    var path = '/' + pathPieces.slice(1).join('/');
    $('#hdfs_browser').osxFinder('navigateToPath', path, hdfsId, true);
  };
  window.onpopstate = function(e) {
    navigateUsingPath();
  };

  errorPopup = function(message) {
    $('<div id="error">' + message +'</div>').popup({
      title: 'Error',
      btns: {
        "Ok": {
          "class": "primary",
          func: function(){
            $().closePopup();
          }
        }
      }
    });
  };

  /*
    # Methods to call on page load
  */
  $(document.body).append(tree_context_menu());
  setup_context_menus();
  paste_buffer = {};
  $('path').live('click', function() {
    $().closePopup();
    var id = $('#top_level .osxSelected').attr('hdfs_id');
    var fullpath = $(this).attr('title');
    fullpath = fullpath.substring(fullpath.indexOf('//') + 2);
    var path = fullpath.substring(fullpath.indexOf('/'));
    $('#hdfs_browser').osxFinder('navigateToPath', path);
    show_dir_props(id, path);
  });
  $('#hdfs_browser').osxFinder({
    done: function() {
      navigateUsingPath();
    },
    navigated: function(e, data) {
      history.pushState({}, '', data.url);
    }
  });

  /* Multiple select */
  var remove_selected = function(keep_selected){
    $.each(columnSelected, function(index, value){
      if (value != keep_selected){
        $(value).removeClass('osxSelected');
      }
    });
  };

  var add_selected = function(group_selected, current){
    $.each(group_selected, function(index, value){
      if (value == current && group_selected.length > 1)
        $(value).removeClass('osxSelected');
      else
        $(value).addClass('osxSelected');
    });
  };

  $(document).on('click', function(e){
    if(e.which == 1){ //checks for left mouse button (needed in FF 3.6)
      var elems = $(e.target).closest('li');

      //click outside of the lists
      if (elems.length == 0 && !ctrlHeld) {
        remove_selected(lastClicked);
        columnSelected = [];
        $('.contextMenu').enableContextMenuItems('#mkdir,#upload,#rename,#dirprops');
      }

      //click a list element
      else {
        var parent = elems.parent();
        if(ctrlHeld) { //CTRL held down
          if (columnSelected.length == 0) {
            add_selected(allSelected, null);
          }
          else if ($(columnSelected[0]).parent()[0] == parent[0]) {
            add_selected(columnSelected, elems[0]);
          }
          else {
            remove_selected(lastClicked);
          }
          columnSelected = $(parent).find('.osxSelected');
          if (columnSelected.length > 1) {
            $('.contextMenu').disableContextMenuItems('#mkdir,#upload,#rename,#dirprops');
          }
          else {
            $('.contextMenu').enableContextMenuItems('#mkdir,#upload,#rename,#dirprops');
          }
          lastClicked = ( columnSelected.length > 0 ? elems[0] : null);
        }
        else { //CTRL not held down
          remove_selected(elems[0]);
          if ($(columnSelected[0]).parent()[0] != parent[0]) {
            $(lastClicked).addClass('osxSelected');
          }
          columnSelected = [];
          $('.contextMenu').enableContextMenuItems('#mkdir,#upload,#rename,#dirprop');
          lastClicked = null;
        }
      }
    }
  });

  // Method for holding down CTRL key
  $(document).on('keydown', function(e) {
    if (!ctrlHeld && (e.ctrlKey || e.which == 224)){
      ctrlHeld = (e.ctrlKey || e.which == 224);
      allSelected = $('#hdfs_browser').find('.osxSelected');
    }
  });

  // Method when CTRL key is released
  $(document).on('keyup', function(e) {
    ctrlHeld = (e.ctrlKey || e.which == 224);
  });
});
