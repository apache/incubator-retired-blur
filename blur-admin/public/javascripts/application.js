// Place your application-specific JavaScript functions and classes here
// This file is automatically included by javascript_include_tag :defaults
$(function(){
    function menuSetup() {               
        $('#menu-bar > ul > li > a').each(function() {
            var content = $(this).next().html();
            if (content) {
                $(this).menu({
                    content: content,
                    flyOut: true,
										maxHeight: 100
               });
               $(this).hover(function(){$(this).parent().toggleClass('ui-state-hover')});
            }
        });
    }
    menuSetup();
});
