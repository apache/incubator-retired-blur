(function( $ ){
  var nameSpace = "osxFinder";

  var methods = {
    init : function( options ) {
      var settings = {
        'lockFirst'         : true,
        'width' : 250
      };
      if ( options ) {
        $.extend( settings, options );
      }

      return this.each(function(){
         
        var $this = $(this),
            data = $this.data(nameSpace);

        // If the plugin hasn't been initialized yet
        if ( ! data ) {
          $(this).data(nameSpace, {
            target : $this,
            settings : settings
          });
          $this.addClass('osxFinder');

          var uls = $this.children('ul')
          $this.children('ul').each(function(ul) {
            var divWrapper = methods._createInnerWindow($this);
            $(this).wrap(divWrapper);
            methods._postProcessInnerWindow($this, $(this).parent('div'));
          });

        }
      });
    },
    _createInnerWindow: function($this) {
      var currentCount = $this.children('div').size();
      var width = $this.data(nameSpace).settings.width;
      return $('<div/>').addClass('innerWindow').css({
        'width': width,
        'left': currentCount*(width+2)
      });
    },
    _ensureLastWindowVisible: function($this) {
      var currentCount = $this.children('div').size();
      var innerWidth = currentCount * ($this.data(nameSpace).settings.width+2)
      var pluginWidth = $this.innerWidth();
      if(innerWidth > pluginWidth) {
        $this.animate({scrollLeft:innerWidth-pluginWidth}, 'slow');
      }
    },
    _postProcessInnerWindow: function($this, innerWindow) {
      innerWindow.find('ul li').click(function(evt){
        $(this).parents('div.innerWindow').nextAll().remove();
        $(this).addClass('osxSelected').siblings().removeClass('osxSelected');
        var innerWindow = methods._createInnerWindow($this);
        $this.append(innerWindow);
        methods._ensureLastWindowVisible($this);

        $.ajax($(this).children('a').attr('href'), {
          'success':function(data) {
            innerWindow.append(data);
            methods._postProcessInnerWindow($this, innerWindow);
          },
          'error': function(data) {
            innerWindow.html(data);
          }
        });
        return false;
      });
      
    },
    destroy : function( ) {
      return this.each(function(){
        var $this = $(this),
            data = $this.data(nameSpace);

        // Namespacing FTW
        data.tooltip.remove();
        $this.removeData(nameSpace);
      })
    }
  };

  $.fn.osxFinder = function( method ) {
    
    if ( methods[method] ) {
      return methods[method].apply( this, Array.prototype.slice.call( arguments, 1 ));
    } else if ( typeof method === 'object' || ! method ) {
      return methods.init.apply( this, arguments );
    } else {
      $.error( 'Method ' +  method + ' does not exist on jQuery.' + nameSpace );
    }    
  
  };

})( jQuery );