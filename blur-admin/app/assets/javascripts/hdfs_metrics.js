//= require d3/d3
//= require flot/flot
//= require flot/jquery.flot.resize.min
//= require_self

$(document).ready(function(){
	//Page constants
	var hdfs_data = {};
	var joinedGraphData = {disk: {metrics:[]}, nodes: {metrics:[]}, block: {metrics:[]}};
	var time_length = 5;
	var refresh_time = 15000;
	var actions = ['disk', 'nodes', 'block'];

	//hash of labels and object lookup strings for the various actions
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
			label_2: "Corrupt Blocks",
			stat_1: "under_replicated",
			stat_2: "corrupt_blocks"
		}
	};

	// Page Methods
	var draw_graph = function(selector, graph_data){
		if (!graph_data.plot)
		{
			graph_data.plot = $.plot(selector, graph_data.metrics, 
			{
				xaxis:
				{
					mode: "time",
					timeformat: "%0m/%0d %H:%M %p"
				}
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
	var request_data = function(id, req_data){
		$.ajax({
			url: Routes.stats_hdfs_path(id),
			type: 'PUT',
			data: req_data,
			success: function(data){
				if (data.length <= 0){
					return;
				}
				if (!hdfs_data[id]){
					hdfs_data[id] = { disk: { metrics: [] }, nodes: { metrics: [] }, block: { metrics: [] } };
				}
				for(var action in hdfs_request_lookup){
					var request_options = hdfs_request_lookup[action];
					var hdfs_data_1 = {label: request_options.label_1, data: []};
					var hdfs_data_2 = {label: request_options.label_2, data: []};
					for( var i in data ){
						var point = data[i];
						var entry_date = new Date(point.created_at).getTime();
						hdfs_data_1.data.push([entry_date, point[request_options.stat_1]]);
						hdfs_data_2.data.push([entry_date, point[request_options.stat_2]]);
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
					}

					var graph_container = $('.graph_instance#' + id).find('.tab-pane#' + action + '_' + id)
					if (graph_container.hasClass('active')){
						draw_graph(graph_container.find('.graph'), hdfs_data[id][action]);
					}
				}
				update_joined_graphs();
			}
		});
	};

	var update_joined_graphs = function(){
		var joinedInstance = $('.joined_instance#joinedGraph:visible');
		if(joinedInstance.length > 0){
			var action = joinedInstance.find('li.active > a').data('action');
			draw_graph(joinedInstance.find('.tab-pane.active > .graph'), joinedGraphData[action]);
		}
	};

	var update_live_graphs = function(){
		$('.graph_instance').each(function(){
			//TODO: check to see if live is checked
			var hdfs_id = $(this).attr('id');
			for (var index = 0; index < actions.length; index++){
				request_data(hdfs_id, {stat_id: hdfs_data[hdfs_id][actions[index]].largest_id});
			}
		});
		setTimeout(function(){
			update_live_graphs();
		}, refresh_time);
	};

	var gather_data_and_draw = function(action){
		var joinedInstance = $('.joined_instance#joinedGraph');
		var container = $('.combined_graph');
		var graphTitles = [];
		$('.graph_instance').each(function(){
			if($(this).find('input[type=checkbox]:checked').length > 0){
				var graphId = parseInt($(this).attr('id'), 10);
				graphTitles.push($(this).find('.graph_title > h3').text());
				if (!joinedGraphData[action].metrics.length > 0){
					joinedGraphData[action].metrics = joinedGraphData[action].metrics.concat(hdfs_data[graphId][action].metrics);
				}
			}
		});
		draw_graph(joinedInstance.find('.tab-pane.active > .graph'), joinedGraphData[action]);
		container.find('.graph_title > h3').text('Composite Graph: ' + graphTitles.join(', '));
	};

	// Page listeners
	$('.graph_instance').on('shown', 'a[data-toggle="tab"]', function(e){
		var instance = $(this).closest('.graph_instance')
		var hdfs_id = instance.attr('id');
		var container = instance.find('.graph');
		var action = $(this).data('action');
		draw_graph(container, hdfs_data[hdfs_id][action]);
	});

	$('.joined_instance').on('shown', 'a[data-toggle="tab"]', function(e){
		var action = $(this).data('action');
		gather_data_and_draw(action);
	});

	$('.create_graph_prompt').on('click', function(e){
		$('.create_graph_prompt').hide();
		$('.combined_graph').show();
		gather_data_and_draw('disk');
	});

	$('.joined_instance#joinedGraph').find('i.icon-remove-sign').on('click', function(e){
		var container = $('.combined_graph');
		container.hide();
		for(var action in joinedGraphData){
			if(joinedGraphData[action].plot){
				joinedGraphData[action].plot.shutdown();
			}
		}
		container.find('.tab-pane > .graph').empty();
		$('.create_graph_prompt').show();
		container.find('.graph_title > h3').text('');
		joinedGraphData = {disk: {metrics:[]}, nodes: {metrics:[]}, block: {metrics:[]}};
	});

	$('.graph_instance').each(function(){
		var hdfs_id = $(this).attr('id');
		request_data(hdfs_id, {stat_mins: time_length});
	});

	// Refresh Timers

	setTimeout(function(){
		update_live_graphs();
	}, refresh_time);
});



	
