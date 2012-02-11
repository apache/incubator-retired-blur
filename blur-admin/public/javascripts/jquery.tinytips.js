			
		$('path').live
			mouseover: () ->
				title = $(this).attr('title') || "No path name found!"
				showTooltip($(this), title)
			mouseleave: () ->
				removeTooltip()
		
		showGraphTooltip = (graph, tipContent) ->
		  tooltip = $('<div id="graphtip" style="display:none"><div id="tipcontent">' + tipContent + '</div></div>')
			
			graphWidth = graph.outerWidth();
			graphHeight = graph.outerHeight();
			tipWidth = tooltip.outerWidth();
			tipHeight = tooltip.outerHeight();
			drawPositionX = (graphWidth / 2) - (tipWidth / 2);
			drawPositionY = (graphHeight / 2) - (tipHeight / 2);

			tooltip.css({top: drawPositionY+'px', left: drawPositionX+'px'});
			$('.radial-graph').append tooltip
			tooltip.fadeIn(400);
		
		removeGraphTooltip = () ->
			$('#graphtip').hide().remove()