//= require d3/d3
//= require flot/flot
//= require flot/jquery.flot.resize.min
//= require_self

$(document).ready(function(){

  // Page constants //

  var hdfs_data = {};
	var time_length = 10;
	var refresh_time = 15000;
	var actions = ['disk', 'nodes', 'block'];

	// Hash of labels and object lookup strings for the various actions //

  var hdfs_request_lookup =
	{
		disk:
		{
			label_1: "Hdfs Disk Capacity (GB)",
			label_2: "Hdfs Disk Usage (GB)",
			stat_1: "capacity",
			stat_2: "used"
		},
		nodes:
		{
			label_1: "Live Nodes",
			label_2: "Dead Nodes",
			stat_1: "live_nodes",
			stat_2: "dead_nodes"
		},
		block:
		{
			label_1: "Under Replicated Blocks",
      label_2: "Missing Blocks",
			stat_1: "under_replicated",
      stat_2: "missing_blocks"
		}
	};

	// Page Methods //

  var draw_graph = function(selector, graph_data){
    if (!graph_data.plot || selector.find(".base").length < 1)
    {
      graph_data.plot = $.plot(selector, graph_data.metrics,
			{
				xaxis:
				{
					mode: "time",
					timeformat: "%0m/%0d %H:%M %p"
				},
        yaxes:[
          { },
          {
            position: 'right'
          }
        ]
			});
		}
		else
		{
			graph_data.plot.setData(graph_data.metrics);
			graph_data.plot.setupGrid();
      graph_data.plot.draw();
		}
	};

	//request graph data
	//id : hdfs id, action: (disk, nodes, block), req_data(optional):
		//req_data.stat_id for data after a certain ID (update)
		//req_data.stat_mins for specifying a different range (overwrite)
	var request_data = function(id, req_data, redraw){
    $.ajax({
			url: Routes.stats_hdfs_path(id),
			type: 'GET',
			data: req_data,
			success: function(data){
        if (data.length <= 0){
          if (!hdfs_data[id]) {
            $.each($('.graph_instance#' + id + ' .graph'), function(index, value) {
              this.innerHTML = 'No data available';
            });
          }
          return;
				}
				if (!hdfs_data[id] || redraw){
          $(".graph_instance").find(".active > .graph").empty();
					hdfs_data[id] = { disk: { metrics: [] }, nodes: { metrics: [] }, block: { metrics: [] } };
				}
				for(var action in hdfs_request_lookup){
          var request_options = hdfs_request_lookup[action];
					var hdfs_data_1 = {label: request_options.label_1, data: []};
          var hdfs_data_2 = {label: request_options.label_2, data: [], yaxis: 2};
          var graph_container = $('.graph_instance#' + id).find('.tab-pane#' + action + '_' + id)

          for( var i in data ){
						var point = data[i];
						var entry_date = new Date(point.created_at).getTime();
            hdfs_data_1.data.push([entry_date - 4*60*60*1000, point[request_options.stat_1]]);
						hdfs_data_2.data.push([entry_date - 4*60*60*1000, point[request_options.stat_2]]);
          }
					//Current implementation is a fixed size queue for storing data
					// Future implementations may allow you to change the range (length of queue, still fixed to a size)
					// Future might also allow to grow the size of the queue overtime (length of queue appends data and never truncates)
					if (req_data && req_data.stat_id){
							var length = data.length;
							hdfs_data[id][action].metrics[0].data.splice(0, length);
							hdfs_data[id][action].metrics[1].data.splice(0, length);
							hdfs_data[id][action].metrics[0].data = hdfs_data[id][action].metrics[0].data.concat(hdfs_data_1.data);
							hdfs_data[id][action].metrics[1].data = hdfs_data[id][action].metrics[1].data.concat(hdfs_data_2.data);
					}	else {
						hdfs_data[id][action].metrics.push(hdfs_data_1, hdfs_data_2);
					}

					if (point){
						hdfs_data[id][action].largest_id = point.id;
            hdfs_data[id][action].max_time = new Date(point.created_at);
            hdfs_data[id][action].min_time = new Date(hdfs_data[id][action].metrics[0].data[0][0] + 4*60*60*1000);
					}
	
					if (graph_container.hasClass('active')){
            draw_graph(graph_container.find('.graph'), hdfs_data[id][action]);
            slider_info_vals(hdfs_data[id][action].min_time, hdfs_data[id][action].max_time, id);
					}
				}
			}
		});
	};

	var update_live_graphs = function(){
		$('.graph_instance').each(function(){
			var hdfs_id = $(this).attr('id');
      request_data(hdfs_id, {stat_id: hdfs_data[hdfs_id][actions[0]].largest_id});
		});
		setTimeout(function(){
			update_live_graphs();
		}, refresh_time);
	};

	// Page listeners //

  $('.graph_instance').on('shown', 'a[data-toggle="tab"]', function(e){
    var instance = $(this).closest('.graph_instance')
		var hdfs_id = instance.attr('id');
		var container = instance.find('.active .graph');
		var action = $(this).data('action');
    if (hdfs_data[hdfs_id]){
      draw_graph(container, hdfs_data[hdfs_id][action]);
    };
	});

	$('.graph_instance').each(function(){
		var hdfs_id = $(this).attr('id');
    request_data(hdfs_id, {stat_mins: time_length});
	});

  $('.loading-spinner').on('ajaxStart', function(){
    $(this).removeClass('hidden');
  });
  $('.loading-spinner').on('ajaxStop', function(){
    $(this).addClass('hidden');
  });

	// Refresh Timers //

	setTimeout(function(){
	  update_live_graphs();
	}, refresh_time);

  // Slider Methods - in progress //
  // slider currently moves over any time period in the last 24 hours //
  // min slide value changes the range of the graph //
  // max slide value currently does not change the graph //
  // TODO: change scale to 2 weeks, allow max to change graph //

  $(".slider").slider({
    range: true,
    min: -1*24*60,//past day
    max: 0,
    values: [-5, 0],
    slide: function(event, ui) {
      date = new Date(hdfs_data[this.id][actions[0]].max_time).getTime();
      minDate = new Date(date + ui.values[0]*1000*60);
      maxDate = new Date(date + ui.values[1]*1000*60);
      slider_info_vals(minDate, maxDate, this.id);
    },
    change: function(event, ui) {
      request_data(this.id, {stat_mins: (-1 * ui.values[0])}, true);
    }
  });

  var slider_info_vals = function(minDate, maxDate, hdfs_id){
    $(".graph_instance#" + hdfs_id + " .slider-info").html( minDate.toLocaleTimeString().slice(0,5)
                           + ' to ' + maxDate.toLocaleTimeString().slice(0,5) );
	};
});
	
