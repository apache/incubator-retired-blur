/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//= require d3/d3
//= require flot/flot
//= require flot/jquery.flot.resize.min
//= require flot/jquery.flot.selection.min
//= require flot/jquery.flot.crosshair.min
//= require jquery.timepicker
//= require jquery.rangeslider.js
//= require underscore
//= require_self

$(document).ready(function(){
  // Datastore for the current data on the plot
  var hdfs_data = {};
  // The max range of data that can be returned (in days)
  var num_days_back = 14;
  // starting range for the graphs
  var default_range = 60 * 24;
  // refresh speed for pulling new info (while live)
	var refresh_time = 20000;
  // timezone offset
  var epoc_offset = new Date().getTimezoneOffset() * 60 * 1000
  // refresh timeout
  var refresh_timer;
  // Different tab actions
	var actions = ['disk', 'nodes', 'block'];
  // Xaxis range 
	// Hash of labels and object lookup strings for the various actions
  var hdfs_request_lookup =
  {
		disk:
		{
			label_1: "Hdfs Disk Capacity (GB) - Left axis <span class='axis-value'></span>",
			label_2: "Hdfs Disk Usage (GB) - Right axis <span class='axis-value'></span>",
			stat_1: "capacity",
			stat_2: "used"
		},
		nodes:
		{
			label_1: "Live Nodes - Left axis <span class='axis-value'></span>",
			label_2: "Dead Nodes - Right axis <span class='axis-value'></span>",
			stat_1: "live_nodes",
			stat_2: "dead_nodes"
		},
		block:
		{
			label_1: "Under Replicated Blocks - Left axis <span class='axis-value'></span>",
      label_2: "Missing Blocks - Right axis <span class='axis-value'></span>",
			stat_1: "under_replicated",
      stat_2: "missing_blocks"
		}
	};

  //------------- Page Methods -------------

  /*
   * Returns a new object used to initialize a new hdfs
   */
  var default_hdfs_data = function(){
    return {
      disk: { metrics: [] },
      nodes: { metrics: [] },
      block: { metrics: [] },
      // Initial range starts at now and goes til 1 day ago (in minutes)
      max: 0,
      min: -default_range,
      // Whether the data is being added to an old set
      updating: false,
      largest_id: 0
    };
  }

  /* 
   * Draw the graph
   * @arg graph_data an object containing all the data related to a graph
   * @arg selector a jquery selector representing the desired location of the graph
   */
  var draw_graph = function(graph_data, selector, min, max){
    // get the min and max
    var now = new Date().getTime();
    min = now + (min * 60 * 1000) - epoc_offset;
    max = now + (max * 60 * 1000) - epoc_offset;

    graph_data.plot = $.plot(selector, graph_data.metrics,
		{
      // Set the xaxis to a time plot
			xaxis: {
        mode: "time",
        timeformat: "%0m/%0d %H:%M %p",
        max: max,
        min: min
      },
      // Set the yaxis to 2 separate scales
      yaxes:[
        { position: 'left', tickDecimals: 1 },
        { position: 'right', tickDecimals: 1 }
      ],
      // Set the legend container
      legend: { container: $(".graph-legend") },
      // Set the crosshair to only show in the y direction
      crosshair: { mode: "x" },
      // Set the grid to hoverable (for value grabbing)
      grid: { hoverable: true, autoHighlight: false },
      // Do not show the lines (looks odd with non contiguous data)
      lines: { show: false },
      // Show the points
      points: { show: true, radius: 2 }
		});
	};

	/* 
   * Request new graph data
	 * @arg id of the hdfs
   * @arg req_data data object for the ajax request
	 *   req_data.stat_id requests data after a certain ID (update)
	 *   req_data.stat_min specifies a min for the range (update / overwrite)
   *   req_data.stat_max specifies a min for the range (overwrite)
   */
	var request_data = function(id, req_data){
    $.ajax({
			url: Routes.stats_hdfs_metric_path(id),
			type: 'GET',
			data: _.extend(req_data, {format: 'json'}),
			success: function(data){
        // If no data was returned then set the no data message
        if (data.length <= 0 && !hdfs_data[id]){
          $.each($('.graph_instance#' + id + ' .graph'), function(index, value) {
            this.innerHTML = 'No data available';
          });
          return;
				}

        // Loop over every action set and build with the returned data
				for(var action in hdfs_request_lookup){
          var request_options = hdfs_request_lookup[action];
          // Create the buffers for parsing the returned data
					var hdfs_data_1 = {label: request_options.label_1, data: []};
          var hdfs_data_2 = {label: request_options.label_2, data: [], yaxis: 2};
          // Grab the correct container from the page
          var graph_container = $('.graph_instance#' + id).find('.tab-pane#' + action + '_' + id);

          // Add all of the data points to the buffers
          for( var i in data ){
						var point = data[i];
						var entry_date = new Date(point.created_at).getTime();
            // Convert the dates to epoc time
            hdfs_data_1.data.push([entry_date - epoc_offset, point[request_options.stat_1]]);
						hdfs_data_2.data.push([entry_date - epoc_offset, point[request_options.stat_2]]);
          }

          // if we are updating the current data set then add the new data
          // NOTE: the data is a fixed size queue (time) therefore data out of view is dropped
					if (req_data && req_data.stat_id){
            var length = hdfs_data[id][action].metrics[0].data.length;
            var now = new Date();
              for(var find = 0; find <= length; find++){
                if (hdfs_data[id][action].metrics[0].data[find] < now + hdfs_data[id].min * 60 * 1000) break;
              }
              
              // remove all data outside of the range
              if(find > 0){
                hdfs_data[id][action].metrics[0].data.splice(0, find);
                hdfs_data[id][action].metrics[1].data.splice(0, find);
              }

							hdfs_data[id][action].metrics[0].data = hdfs_data[id][action].metrics[0].data.concat(hdfs_data_1.data);
							hdfs_data[id][action].metrics[1].data = hdfs_data[id][action].metrics[1].data.concat(hdfs_data_2.data);
					// otherwise we are loading a new dataset (clear and then add)
          }	else {
            hdfs_data[id][action].metrics = []
						hdfs_data[id][action].metrics.push(hdfs_data_1, hdfs_data_2);
					}

          // Mark the largest id for update requests (only if it is the largest)
					if (point && hdfs_data[id].largest_id <  point.id) hdfs_data[id].largest_id = point.id

          // Only redraw the active graph
					if (graph_container.hasClass('active')){
            draw_graph(hdfs_data[id][action], graph_container.find('.graph'), hdfs_data[id].min, hdfs_data[id].max);
					}

          // Sync up the sliders / pickers to the newest date returned (may not change depending on ranges)
          sync_slider(id);
          sync_date_fields(id);
				}
			}
		});
	};

  /* 
   * Loops over every graph instance and requests the new data 
   * @arg id id of the hdfs we are targetting
   */
	var update_live_graphs = function(id){
    $('.graph_instance').each(function(){
      // Grab the id of the current graph
			var hdfs_id = $(this).attr('id');
      // if an id is defined then we only want to update the graph with that id
      if (id && hdfs_id !== id) return;
      
      // if the max is now
      if (hdfs_data[hdfs_id].max === 0){
        // Clear the timeout of the old refresh
        clearTimeout(refresh_timer);
        // if we are grabbing an update to a current range
        if (hdfs_data[hdfs_id].updating){
          request_data(hdfs_id, {stat_id: hdfs_data[hdfs_id].largest_id});
        // otherwise it is a newly selected range with a max of 0
        } else {
          request_data(hdfs_id, {stat_min: -hdfs_data[hdfs_id].min});
          // set the updating to true because the max is now
          hdfs_data[hdfs_id].updating = true;
        }
        refresh_timer = setTimeout(update_live_graphs, refresh_time);
      // otherwise request the range
      } else {
        request_data(hdfs_id, {stat_min: -hdfs_data[hdfs_id].min, stat_max: -hdfs_data[hdfs_id].max});
      }
    });
	};

  /*
   * Sets the time fields to the minDate and maxDate
   */
  var sync_slider = function(hdfs_id){
    // the range values are stored as slider offsets simply set the slider to those values
    var range_values = [hdfs_data[hdfs_id].min, hdfs_data[hdfs_id].max]
    $('.graph_instance#' + hdfs_id + ' .slider').dragslider('option', 'values', range_values);
  };

  /*
   * Sets the time fields to match the slider
   * @arg hdfs_id target hdfs
   * @arg min(optional) min value for date picker
   * @arg max(optional) max value for date picker
   */
  var sync_date_fields = function(hdfs_id, min, max) {
    // get the current time
    var now = new Date().getTime();

    // grab the min date and max dates
    min = typeof min !== "undefined" ? min : hdfs_data[hdfs_id].min;
    max = typeof max !== "undefined" ? max : hdfs_data[hdfs_id].max;
    var min_date = now + min * 60 * 1000;
    var max_date = now + max * 60 * 1000;

    // set the time pickers to the correct time and max/min time
    var graph = $(".graph_instance#" + hdfs_id)
    graph.find(".min-date")
      .datetimepicker("option", "maxDate", new Date(max_date))
      .datetimepicker("setDate", new Date(min_date));
    graph.find(".max-date").datetimepicker("option", "maxDate", new Date())
      .datetimepicker("option", "minDate", new Date(min_date))
      .datetimepicker("setDate", new Date(max_date));
  };

	//------------- Page Events -------------

  // Request the first set of data for every graph (starts the page)
  // Uses the initial time length on
  $('.graph_instance').each(function(){
    var hdfs_id = $(this).attr('id');
    hdfs_data[hdfs_id] = default_hdfs_data();
    update_live_graphs(hdfs_id);
  });

  // Draws the new graph when a new tab is shown
  $('.graph_instance').on('shown', 'a[data-toggle="tab"]', function(e){
    var instance = $(this).closest('.graph_instance')
		var hdfs_id = instance.attr('id');
		var container = instance.find('.active > .graph');
		var action = $(this).data('action');
    if (hdfs_data[hdfs_id]){
      draw_graph(hdfs_data[hdfs_id][action], container, hdfs_data[hdfs_id].min, hdfs_data[hdfs_id].max);
    };
	});

  // Show the loading spinner when there is an ajax request taking place
  $('.loading-spinner').on('ajaxStart', function(){
    $(this).removeClass('hidden');
  }).on('ajaxStop', function(){
    $(this).addClass('hidden');
  });

    // Update the legend on crosshair show     
  $(".graph").bind("plothover",  function (event, pos, item) {    
    // grab the plot from the global store
    var graph_definition = $(event.target).closest('.active').attr('id');
    var pieces = graph_definition.split('_');
    var plot = hdfs_data[pieces[1]][pieces[0]].plot
    var show_offset = 15; // (in minutes)
     
    // get the datasets for searching
    var axes = plot.getAxes();
    var datasets = plot.getData();
    var series = datasets[0];

    // legend holder
    var legends = $(this).closest('.graph_data').find('.axis-value');

    // break if we are hovering off the viewport
    if (pos.x < axes.xaxis.min || pos.x > axes.xaxis.max ||
        pos.y < axes.yaxis.min || pos.y > axes.yaxis.max){
      legends.text('');
      return;
    }

    // find the nearest points, x-wise
    // ToDo: Change this so that it only reads a val if it is within a certain distance
    for (index = 0; index < series.data.length - 1; ++index)
      if (series.data[index][0] + show_offset * 60 * 1000 > pos.x)
        break;

    // absolute distance from mouse to nearest point
    var current_delta = Math.abs(pos.x - series.data[index][0]);

    // if the point is more than 5 min away draw nothing
    if ((current_delta / (1000 * 60)) > show_offset){
      legends.text('');
      return;
    }
            
    // draw to the legend
    for (plot_index = 0; plot_index < datasets.length; plot_index++){
      var point = datasets[plot_index].data[index];
      legends.eq(plot_index).text('(' + point[1].toFixed(2) + ')');
    }
  });

  //------------- Page Elements -------------

  /* 
   * Creates the date slider
   */
  $(".slider").dragslider({
    range: true,
    // allows for dragging of the range
    rangeDrag: true,
    // Max range
    min: -1 * 60 * 24 * num_days_back,
    // Min range (now)
    max: 0,
    // Starting selected range is the default
    values: [-1 * default_range, 0],
    // A new value has been picked
    stop: function(event, ui) {
      // grab the current hdfs_id
      var hdfs_id = $(this).closest('.graph_instance').attr('id');
      // set the values for the hdfs
      hdfs_data[hdfs_id].min = ui.values[0];
      hdfs_data[hdfs_id].max = ui.values[1];
      // set the updating to false because a range was selected
      hdfs_data[hdfs_id].updating = false;
      // Sync the date fields with the range selected
      sync_date_fields(hdfs_id);
      // update the graph for this hdfs
      update_live_graphs(hdfs_id);
    },
    // Update the date values while sliding (feedback for user)
    slide: function(event, ui){
      // grab the current hdfs_id
      var hdfs_id = $(this).closest('.graph_instance').attr('id');
      // Sync the date fields with the range selected
      sync_date_fields(hdfs_id, ui.values[0], ui.values[1]);
    }
  });

  $(".min-date").datetimepicker({
    minDate: -1 * num_days_back,
    defaultDate: -1,
    onSelect: function (selectedDateTime){
      // grab the current hdfs_id
      var hdfs_id = $(this).closest('.graph_instance').attr('id');
      // get the new date as minutes
      var min_date = Math.round((new Date().getTime() - new Date(selectedDateTime).getTime()) / (-1 * 1000 * 60))
      // set the new min range
      hdfs_data[hdfs_id].min = min_date;
      // set the updating to false because a range was selected
      hdfs_data[hdfs_id].updating = false;
      $(".max-date").datetimepicker("option", "minDate", new Date(selectedDateTime));
      // reload the data
      sync_slider(hdfs_id);
      update_live_graphs(hdfs_id);
    }
  })

  // Create the Date pickers for each graph
  $(".max-date").datetimepicker({
    // Earliest date available is the max of min-date
    minDate: -1 * default_range / (60 * 24),
    defaultDate: 0,
    onSelect: function (selectedDateTime){
      // grab the current hdfs_id
      var hdfs_id = $(this).closest('.graph_instance').attr('id');
      // get the new date as minutes
      var max_date = Math.round((new Date().getTime() - new Date(selectedDateTime).getTime()) / (-1 * 1000 * 60));
      // set the new min range
      hdfs_data[hdfs_id].max = max_date;
      // set the updating to false because a range was selected
      hdfs_data[hdfs_id].updating = false;
      $(".min-date").datetimepicker("option", "maxDate", new Date(selectedDateTime));
      // reload the data
      sync_slider(hdfs_id);
      update_live_graphs(hdfs_id);
    }
  });
});
