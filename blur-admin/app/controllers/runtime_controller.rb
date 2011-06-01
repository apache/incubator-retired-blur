class RuntimeController < ApplicationController
  require 'thrift/blur'
  def show
    client = setup_thrift
    @tables = client.tableList()
    @running_queries = BlurQueries.all
    close_thrift

    respond_to do |format|
      format.html
      format.json
    end
  end

  def queries
    if params[:table] == "all"
      running_queries = BlurQueries.all
    else
      running_queries = BlurQueries.where(:table_name => params[:table]).all
    end
    @table = []
    columns = [:query_string, :super_query_on, :start, :fetch_num, :cpu_time, :real_time, :complete, :uuid, :running, :table_name, :updated_at]
    running_queries.each do |query|
      row = []
      columns.each do |column|
        if column == :complete
          if query[:complete] then row.push "Complete"
          elsif query[:running] then row.push "Running"
          elsif query[:interrupted] then row.push "Interrupted"
          else row.push "???"
          end
        else
          row.push query[column]
        end
      end
      @table.push row
    end
    render :json => @table
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

