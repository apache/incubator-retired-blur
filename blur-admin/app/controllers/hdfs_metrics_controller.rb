class HdfsMetricsController < ApplicationController
  def index
    @hdfs_index = Hdfs.all
  end

  def stats
    @results = hdfs_stat_select [:present_capacity, :dfs_used_real, :live_nodes, :dead_nodes, :under_replicated, :corrupt_blocks, :missing_blocks]
    render :json => @results, :methods => [:capacity, :used], :except => [:present_capacity, :dfs_used]
  end

  private
  def hdfs_stat_select(properties)
    hdfs = Hdfs.find params[:id]
    properties = [:id, :created_at] + properties
    minutes = params[:stat_mins].nil? ? 1 : params[:stat_mins].to_i

    if params[:stat_id]
      where_clause = "id > #{params[:stat_id]}"
    else
      where_clause = "created_at >= '#{minutes.minute.ago}'"
      where_clause += " and created_at < '#{params[:max_mins].to_i.minute.ago}'" if params[:max_mins]
    end

    stat_number = hdfs.hdfs_stats.where(where_clause).select("count(*) as count")
    if stat_number.count >= 700
      remove_number = (stat_number.count / 700).floor
      where_clause += "and hdfs_stats.id % #{remove_number} = 0"
    end

    return hdfs.hdfs_stats.where(where_clause).select(properties)
  end
end
