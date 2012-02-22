$(document).ready ->
	#scoped methods
	request_disk_data = (id, req_data) ->
		data_bucket = hdfs_data[id] = {} if !hdfs_data[id]
		data_bucket.disk = {}
		$.ajax 'http://localhost:3000/hdfs_metrics/' + id + '/disk',
			type: 'PUT'
			data: req_data
			success: (data) ->
				hdfs_capacity = 
					label: "Hdfs Disk Capacity"
					data: []
				hdfs_usage = 
					label: "Hdfs Disk Usage"
					data: []
				for point, i in data
					entry_date = new Date(point.created_at).getTime()
					hdfs_capacity.data.push [entry_date, point.present_capacity]
					hdfs_usage.data.push [entry_date, point.dfs_used]
				hdfs_data[id].disk.data = [hdfs_capacity, hdfs_usage]
				hdfs_data[id].disk.largest_id = point.id

	#scoped variables
	hdfs_data = {}

	$('.graph_instance').each (index) ->
		hdfs_id = $(this).attr('id')
		request_disk_data(hdfs_id)

	
