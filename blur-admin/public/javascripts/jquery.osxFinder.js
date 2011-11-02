(function( $ ){
  $.widget('ui.osxFinder',{
    options: {
        lockFirst : true,
        width : 250
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
      $('div.innerWindow>ul>li',$('#'+el[0].id)).live('click', function() {
        var evtEl = $(this);
        if(!evtEl.hasClass('osxSelected')) {
          evtEl.parents('div.innerWindow').nextAll().remove();
          evtEl.addClass('osxSelected').siblings().removeClass('osxSelected');
          var innerWindow = self._createInnerWindow();
          el.append(innerWindow);
          self._ensureLastWindowVisible();

          var url = evtEl.children('a').attr('href');
          $.ajax(url, {
            success:function(data) {
              innerWindow.append(data);
              self._trigger("added", null, innerWindow);
            },
            error: function(data) {
              innerWindow.html(data);
            }
          });
        }
        return false;
      });
    },
    _createInnerWindow: function() {
      var currentCount = this.element.children('div').size();
      var width = this.options.width;
      return $('<div/>').addClass('innerWindow').css({
        'width': width,
        'left': (currentCount*(width+1))+2,
        'top' : 2
      });
    },
    _ensureLastWindowVisible: function() {
      var currentCount = this.element.children('div').size();
      var innerWidth = currentCount * (this.options.width+2)
      var pluginWidth = this.element.innerWidth();
      if(innerWidth > pluginWidth) {
        this.element.animate({scrollLeft:innerWidth-pluginWidth}, 'slow');
      }
    }
  });

})( jQuery );