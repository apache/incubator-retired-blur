//= require d3/d3
//= require flot/flot
//= require_self

$(document).ready(function(){
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

	var draw_graph = function(id, graph_data){
		if (!graph_data.plot)
		{
			var hdfs_active_graph = $('.graph_instance#' + id).find('.tab-pane.active > .graph');
			graph_data.plot = $.plot(hdfs_active_graph, graph_data.metrics, 
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
	var request_data = function(id, action, req_data){
		$.ajax({
			url: 'hdfs_metrics/' + id + '/' + action,
			type: 'PUT',
			data: req_data,
			success: function(data){
				var request_options = hdfs_request_lookup[action];
				var hdfs_stat1 = { label: request_options.label_1, data: [] };
				var hdfs_stat2 = { label: request_options.label_2, data: [] };
				//format the returned data into something that flot can use
				for( i in data )
				{
					var point = data[i];
					var entry_date = new Date(point.created_at).getTime();
					hdfs_stat1.data.push([entry_date, point[request_options.stat_1]]);
					hdfs_stat2.data.push([entry_date, point[request_options.stat_2]]);
				}
				//if the reqdata object and the property stat id are set
				//then we are updating old data
				if (req_data && req_data.stat_id)
				{
					var length = hdfs_stat1.data.length;
					hdfs_data[id][action].metrics[0].data.splice(0, length);
					hdfs_data[id][action].metrics[1].data.splice(0, length);
					hdfs_data[id][action].metrics[0].data = hdfs_data[id][action].metrics[0].data.concat(hdfs_stat1.data);
					hdfs_data[id][action].metrics[1].data = hdfs_data[id][action].metrics[1].data.concat(hdfs_stat2.data);
				}
				else
				{
					if (!hdfs_data[id])
					{
						hdfs_data[id] = { disk: {}, nodes: {}, block: {} };
					}
					hdfs_data[id][action].metrics = [hdfs_stat1, hdfs_stat2];
				}
				if (point){
					hdfs_data[id][action].largest_id = point.id;
				}
				draw_graph(id, hdfs_data[id][action]);
			}
		});
	};

	var hdfs_data = {}

	$('.graph_instance').on('show', 'a[data-toggle="tab"]', function(e){
		var hdfs_id = $(this).closest('.graph_instance').attr('id');
		var action = $(this).attr('href').slice(1);
		request_data(hdfs_id, action);
	});

	var update_graphs = function(element){
		var hdfs_id = $(element).attr('id');
		var action = $(element).find('li.active > a').attr('href').slice(1);
		request_data(hdfs_id, action, {stat_mins: time_length});
	};

	var update_live_graphs = function(){
		$('.graph_instance').each(function(){
			//TODO: check to see if live is checked
			var hdfs_id = $(this).attr('id');
			var action = $(this).find('li.active > a').attr('href').slice(1);
			var reqData = !hdfs_data[hdfs_id][action].largest_id !== null ?
				{stat_id: hdfs_data[hdfs_id][action].largest_id} : {stat_mins: time_length};
			request_data(hdfs_id, action, {stat_mins: reqData})
		});
		setTimeout(function(){
			update_live_graphs()
		}, 60000);
	};

	var time_length = 5;

	$('.graph_instance').each(function(){
		update_graphs(this);
	});

	setTimeout(function(){
		update_live_graphs()
	}, 60000);
});



	
