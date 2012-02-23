$(document).ready ->
	#hash of labels and object lookup strings for the various actions
	hdfs_request_lookup = 
		disk:
			label_1: "Hdfs Disk Capacity (GB)"
			label_2: "Hdfs Disk Usage (GB)"
			stat_1: "capacity"
			stat_2: "used"
		nodes:
			label_1: "Live Nodes"
			label_2: "Dead Nodes"
			stat_1: "live_nodes"
			stat_2: "dead_nodes"
		block:
			label_1: "Under Replicated Blocks"
			label_2: "Corrupt Blocks"
			stat_1: "under_replicated"
			stat_2: "corrupt_blocks"
	draw_graph = (id, graph_data) ->
		if !graph_data.plot
			graph_data.plot = $.plot $('.graph_instance#' + id).find('.tab-pane.active > .graph'), graph_data.metrics,
				xaxis:
					mode: "time"
					timeformat: "%0m/%0d %H:%M %p"
		else
			graph_data.plot.setData graph_data.metrics
			graph_data.plot.setupGrid()
			graph_data.plot.draw()

	#request graph data
	#id : hdfs id, action: (disk, nodes, block), req_data(optional):
		#req_data.stat_id for data after a certain ID (update)
		#req_data.stat_days for specifying a different range (overwrite
	request_data = (id, action, req_data) ->
		$.ajax 'http://localhost:3000/hdfs_metrics/' + id + '/' + action,
			type: 'PUT'
			data: req_data
			success: (data) ->
				request_options = hdfs_request_lookup[action]
				hdfs_stat1 = label: request_options.label_1, data: []
				hdfs_stat2 = label: request_options.label_2, data: []
				#format the returned data into something that flot can use
				for point, i in data
					entry_date = new Date(point.created_at).getTime()
					hdfs_stat1.data.push [entry_date, point[request_options.stat_1]]
					hdfs_stat2.data.push [entry_date, point[request_options.stat_2]]
				#if the reqdata object and the property stat id are set
				#then we are updating old data
				if req_data.stat_id
					length = hdfs_stat1.data.length
					hdfs_data[id][action].metrics[0].data.splice(0, length)
					hdfs_data[id][action].metrics[1].data.splice(0, length)
					hdfs_data[id][action].metrics[0].data = hdfs_data[id][action].metrics[0].data.concat hdfs_stat1.data
					hdfs_data[id][action].metrics[1].data = hdfs_data[id][action].metrics[1].data.concat hdfs_stat2.data
				else
					hdfs_data[id] = disk: {}, nodes: {}, block: {} if !hdfs_data[id]
					hdfs_data[id][action].metrics = [hdfs_stat1, hdfs_stat2]
				hdfs_data[id][action].largest_id = point.id if point
				draw_graph(id, hdfs_data[id][action])

	#scoped variables
	hdfs_data = {}

	$('.graph_instance').on 'show', 'a[data-toggle="tab"]', (e) ->
		hdfs_id = $(this).closest('.graph_instance').attr('id')
		action = $(this).attr('href').slice(1)
		request_data(hdfs_id, action)

	update_graphs = (element, days) ->
		hdfs_id = $(element).attr('id')
		action = $(element).find('li.active > a').attr('href').slice(1)
		request_data(hdfs_id, action, {stat_days: days})

	update_live_graphs = () ->
		$('.graph_instance').each ->
			#do some if live checking
			#if $(this).find('.live_updating').attr('checked')
				#then include everything below
			hdfs_id = $(this).attr('id')
			action = $(this).find('li.active > a').attr('href').slice(1)
			request_data(hdfs_id, action, {stat_id: hdfs_data[hdfs_id][action].largest_id})
		setTimeout ->
			update_live_graphs()
		, 60000

	$('.graph_instance').each ->
		update_graphs(this, 1)

	setTimeout ->
		update_live_graphs()
	, 60000



	
