// Place your application-specific JavaScript functions and classes here
// This file is automatically included by javascript_include_tag :defaults
$(function(){
    function menuSetup() {  
	      var length = $('#menu-bar > ul > li > a').length;
        $('#menu-bar > ul > li > a').each(function(index) {
            var content = $(this).next().html();
            if (content) {
                $(this).menu({
                    content: content,
                    flyOut: true,
										maxHeight: 100
               });
               $(this).hover(function(){$(this).parent().toggleClass('ui-state-hover')});
							if(index ==0)
							{
								$(this).addClass('ui-corner-left');
							}
							
							if(index ==length -1)
							{
								$(this).addClass('ui-corner-right');								
							}
            }
        });
    }
    menuSetup();
		$('#widgets').sortable();
});
