class RuntimeController < ApplicationController
  require 'thrift/blur'
  def show
    client = setup_thrift
    @tables = client.tableList()
    @running_queries = BlurQueries.all
    close_thrift
  end

  def cancel
    client = setup_thrift
    client.cancelQuery(params[:table], params[:uuid])
    close_thrift
  end

  def query_time_cpu
    curr_cpu_times = []
    if (params[:table] == "all")
      curr_queries = BlurQueries.all
    else
      curr_queries = BlurQueries.where(:table_name => params[:table]).all
    end
    curr_queries.each do |a|
      curr_cpu_times.push(a.cpu_time)
    end

    render :json => curr_cpu_times
  end

   def query_time_real
    curr_real_times = []
    if (params[:table] == "all")
      curr_queries = BlurQueries.all
    else
      curr_queries = BlurQueries.where(:table_name => params[:table]).all
    end
    curr_queries.each do |a|
      curr_real_times.push(a.real_time)
    end

    render :json => curr_real_times
  end

end

