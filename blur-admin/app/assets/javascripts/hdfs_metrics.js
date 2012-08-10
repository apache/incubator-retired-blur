//= require d3/d3
//= require flot/flot
//= require flot/jquery.flot.resize.min
//= require_self

$(document).ready(function(){

  // Page constants //

  var hdfs_data = {};
	var time_length = 60;
	var refresh_time = 20000;
  var slider_max = {};
  var noRequest = false;
	var actions = ['disk', 'nodes', 'block'];

	// Hash of labels and object lookup strings for the various actions //

  var hdfs_request_lookup =
	{
		disk:
		{
			label_1: "Hdfs Disk Capacity (GB) - left axis",
			label_2: "Hdfs Disk Usage (GB) - right axis",
			stat_1: "capacity",
			stat_2: "used"
		},
		nodes:
		{
			label_1: "Live Nodes - left axis",
			label_2: "Dead Nodes - right axis",
			stat_1: "live_nodes",
			stat_2: "dead_nodes"
		},
		block:
		{
			label_1: "Under Replicated Blocks - left axis",
      label_2: "Missing Blocks - right axis",
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
          { tickDecimals: 0 },
          {
            position: 'right',
            tickDecimals: 0
          }
        ],
        legend:{ container: $(".graph-legend") }
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
      //for overwrite, also set redraw = true (optional)
	var request_data = function(id, req_data, redraw){
    $.ajax({
			url: Routes.stats_hdfs_metric_path(id),
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
					// Also allows you to change the range (length of queue, still fixed to a size)
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

          // Redraws active graph
					if (graph_container.hasClass('active')){
            draw_graph(graph_container.find('.graph'), hdfs_data[id][action]);
            set_slider_info(hdfs_data[id][action].min_time, hdfs_data[id][action].max_time, id);
					}
				}
			}
		});
	};

  var get_recent_stat = function(id){
    $.ajax({
			url: Routes.most_recent_stat_hdfs_metric_path(id),
			type: 'GET',
			success: function(data){
        slider_max[id] = new Date(data.created_at);
      }
    });
  };

	var update_live_graphs = function(){
    $('.graph_instance').each(function(){
			var hdfs_id = $(this).attr('id');
      var sliderVals = $(".graph_instance#" + hdfs_id + " .slider").slider('option', 'values');
      var sliderMin = $(".graph_instance#" + hdfs_id + " .slider").slider('option', 'min');
      if (sliderVals[1] == 0 || sliderMin == sliderVals[0]) {
        request_data(hdfs_id, {stat_id: hdfs_data[hdfs_id][actions[0]].largest_id});
      }
      else {
        noRequest = true;
        set_slider_vals_to_info(hdfs_id);
      }
      get_recent_stat(hdfs_id);
		});
    timer = setTimeout(update_live_graphs, refresh_time);
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
    get_recent_stat(hdfs_id);
	});

  $('.loading-spinner').on('ajaxStart', function(){
    $(this).removeClass('hidden');
  });
  $('.loading-spinner').on('ajaxStop', function(){
    $(this).addClass('hidden');
  });

	// Refresh Timers //

	timer = setTimeout(update_live_graphs, refresh_time);

  // Slider - in progress //
  // Slider currently moves over any time period in the last 24 hours //
  // Change the slider min val to get a different range, make sure that datepicker matches this range
  // TODO
  // Allow for scale to go back up to 2 weeks //
  //   Flot recommends not going over 1,000 data points. We hit 1,000 if the scale is increased //
  //   to about 4 hours (need to consolidate data after that) //
  // Add functionality of grabbing center of slider and draging range //
  // Look at other useful functionalities from flot //
  //   Suggestion: tracking curves with crosshair //
  // Change time to AM and PM (instead of military time //

  // Creates slider
  // Slide: changes the time fields as the slider is dragged
  // Change: requests data for the graph when the slider is released
  $(".slider").slider({
    range: true,
    min: -1*60*24,//past 24 hours in minutes **CHANGE TO GET DIF TIME SPAN (up to 2 weeks available - match slider)**
    max: 0,
    values: [-1 * time_length, 0],
    slide: function(event, ui) {
      set_slider_info_to_vals(ui.values, this.id);
    },
    change: function(event, ui) {
      if (ui.values[1] == 0){
        request_data(this.id, {stat_mins: (-1 * ui.values[0])}, true);
      }
      else if (!noRequest) {
        request_data(this.id, {stat_mins: (-1 * ui.values[0]), max_mins: (-1 * ui.values[1])}, true);
        noRequest = false;
      }
   }
  });

  $(".min-date, .max-date").datepicker({
    minDate: -1,//past 24 hours **CHANGE TO GET DIF TIME SPAN (up to 2 weeks available - match slider)**
    maxDate: 0
  });

  // Takes in the min and max values for the time fields as dates
  // Sets the time fields to the minDate and maxDate
  var set_slider_info = function(minDate, maxDate, hdfs_id){
    minHour = minDate.getHours().toString();
    minMinutes = minDate.getMinutes().toString();
    if (minMinutes.length == 1) {
      minMinutes = "0" + minMinutes;
    }
    maxHour = maxDate.getHours().toString();
    maxMinutes = maxDate.getMinutes().toString();
    if (maxMinutes.length == 1) {
      maxMinutes = "0" + maxMinutes;
    }
    $(".graph_instance#" + hdfs_id + " .min-hour")[0].value = minHour;
    $(".graph_instance#" + hdfs_id + " .min-minutes")[0].value = minMinutes;
    $(".graph_instance#" + hdfs_id + " .max-hour")[0].value = maxHour;
    $(".graph_instance#" + hdfs_id + " .max-minutes")[0].value = maxMinutes;

    $(".graph_instance#" + hdfs_id + " .min-date").datepicker('setDate', minDate);
    $(".graph_instance#" + hdfs_id + " .max-date").datepicker('setDate', maxDate);
	};

  // Sets the time fields to match the slider
  // Have to pass in vals - used for 'slide' in slider (which does not change the slider values)
  var set_slider_info_to_vals = function(vals, hdfs_id) {
    maxSliderVal = slider_max[hdfs_id].getTime();
    if (vals[1] > 0)
      vals[1] = 0;
    minDate = new Date(maxSliderVal + vals[0]*1000*60);
    maxDate = new Date(maxSliderVal + vals[1]*1000*60);
    set_slider_info(minDate, maxDate, hdfs_id);
  };

  // Set the slider to match the change in the time fields
  // If the time in the time fields is invalid, it will instead reset the time fields to match the slider
  var set_slider_vals_to_info = function(hdfs_id) {
    maxSliderVal = slider_max[hdfs_id].getTime();;
    minDate = $(".graph_instance#" + hdfs_id + " .min-date").datepicker('getDate');
    minDate.setHours($('.min-hour')[0].value);
    minDate.setMinutes($('.min-minutes')[0].value);
    maxDate = $(".graph_instance#" + hdfs_id + " .max-date").datepicker('getDate');
    maxDate.setHours($('.max-hour')[0].value);
    maxDate.setMinutes($('.max-minutes')[0].value);

    vals = [];
    vals[0] = (maxSliderVal - minDate.getTime()) / (-1 * 60 * 1000);
    vals[1] = (maxSliderVal - maxDate.getTime()) / (-1 * 60 * 1000);

    min =$('.graph_instance#' + hdfs_id + ' .slider').slider('option', 'min');
    if (!isNaN(vals[0]) && !isNaN(vals[1]) && vals[0] > min && vals[1] >= vals[0] && vals[1] <= 0){
      $('.graph_instance#' + hdfs_id + ' .slider').slider('option', 'values', vals);
    }
    else {
      vals = $('.graph_instance#' + hdfs_id + ' .slider').slider('option', 'values');
      set_slider_info_to_vals(vals, hdfs_id);
    }
  };

  // Listener for 'Redraw' button
  $('.slider-redraw').on('click', function(e){
    set_slider_vals_to_info($(this).parent()[0].id);
  });
});
