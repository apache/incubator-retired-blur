(function( $ ){
  $.widget('ui.osxFinder',{
    options: {
        lockFirst : true,
        width : 250,
        baseUrl : '/'
      },
    _create: function() {
      var self = this,
        o = self.options,
        el = self.element;
      el.addClass('osxFinder');
      el.children('ul').each(function() {
        var divWrapper = self._createInnerWindow();
        $(this).wrap(divWrapper);
      });
      el.on('click', 'div.innerWindow > ul > li', function(e) {
        e.preventDefault();
        var evtEl = $(this);
        if(!evtEl.hasClass('osxSelected')) {
          evtEl.parents('div.innerWindow').nextAll().remove();
          evtEl.addClass('osxSelected').siblings().removeClass('osxSelected');
          var url = evtEl.children('a').attr('href');
          self._requestNextFinderWindow(url);
          self._trigger("navigated", null, {'url':url.replace('expand', 'show')});
        }
      });
      self._trigger("done");
    },
    _createInnerWindow: function() {
      var currentCount = this.element.children('div.innerWindow').size();
      var width = this.options.width;
      return $('<div/>').addClass('innerWindow').css({
        'width': width,
        'left': (currentCount * (width + 1)) + 2,
        'top' : 2
      });
    },
    _ensureLastWindowVisible: function() {
      var currentCount = this.element.children('div').size();
      var innerWidth = currentCount * (this.options.width + 2);
      var pluginWidth = this.element.innerWidth();
      if(innerWidth > pluginWidth) {
        this.element.animate({scrollLeft:innerWidth-pluginWidth}, 'slow');
      }
    },
    _requestNextFinderWindow: function(url, async){
      var self = this;
      var innerWindow = this._createInnerWindow();
      $.ajax(url, {
        async: async,
        success:function(data) {
          innerWindow.append(data);
          self.element.append(innerWindow);
          self._ensureLastWindowVisible();
          self._trigger("added", null,{"innerWindow":innerWindow,"url":url + "/"});
        },
        error: function(data) {
          innerWindow.html("error retrieving [" + url + "]");
        }
      });
      return innerWindow;
    },
    navigateToPath: function(path, id, silence) {
      var el = this.element;
      var self = this;
      var hdfsId = id || el.find('#top_level > .osxSelected').attr('hdfs_id');
      var pathPieces = path.split('/').filter(function(member){return member !== '';});     
      var currentPath = '';
      
      if (typeof hdfsId === 'undefined'){
        throw 'No HDFS was selected or specified'
      }
      
      if (id){
        var selectedHdfs = el.find('#top_level li[hdfs_id=' + id + ']');
        if(!selectedHdfs.hasClass('osxSelected')){
          el.find('.innerWindow:not(:first-child)').remove();
          selectedHdfs.addClass('osxSelected');
          self._requestNextFinderWindow('/hdfs/' + hdfsId + '/expand', false);
        } 
      }
      
      for( pieceIndex in pathPieces ){
        var piece = pathPieces[pieceIndex];
        currentPath += '/' + piece;
        var selectedFolder = el.find('li[hdfs_path="' + currentPath + '"]');
        if(!selectedFolder.length > 0){
          throw "Folder does not exist";
        }
        if(!selectedFolder.hasClass('osxSelected')){
          if (selectedFolder.siblings('.osxSelected').length > 0){
            selectedFolder.closest('.innerWindow').nextAll().remove();
          }
          selectedFolder.addClass('osxSelected').siblings().removeClass('osxSelected');
          var url = selectedFolder.find('a').attr('href');
          self._requestNextFinderWindow(url, false);
        }
      }
      var navBarUrl = '/hdfs/' + hdfsId + '/show' + currentPath;
      if(!silence){
        self._trigger("navigated", null, {'url':navBarUrl});
      }
    }
  });

})( jQuery );