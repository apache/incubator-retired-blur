class HdfsMetricsController < ApplicationController
  def index
    @hdfs_index = Hdfs.all
  end

  def stats
    @results = hdfs_stat_select [:present_capacity, :dfs_used, :live_nodes, :dead_nodes, :under_replicated, :corrupt_blocks, :missing_blocks]
    render :json => @results, :methods => [:capacity, :used], :except => [:present_capacity, :dfs_used]
  end

  private
  def hdfs_stat_select(properties)
    hdfs = Hdfs.find params[:id]
    properties = [:id, :created_at] + properties
    minutes = params[:stat_mins].nil? ? 1 : params[:stat_mins].to_i

    if params[:stat_id]
      where_clause = "id > #{params[:stat_id]}"
    elsif params[:max_mins]
      max_min = params[:max_mins].to_i
      where_clause = "created_at >= '#{minutes.minute.ago}' and created_at < '#{max_min.minute.ago}'"
    else
      where_clause = "created_at >= '#{minutes.minute.ago}'"
    end

    return hdfs.hdfs_stats.where(where_clause).select(properties)
  end
end
