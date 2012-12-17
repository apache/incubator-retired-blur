class HdfsMetricsController < ApplicationController
  respond_to :html, :only => [:index]
  respond_to :json, :only => [:stats]

  def index
    @hdfs_index = Hdfs.all
    respond_with(@hdfs_index)
  end

  def stats
    @results = hdfs_stat_select [:present_capacity, :dfs_used_real, :live_nodes, :dead_nodes, :under_replicated, :corrupt_blocks, :missing_blocks]
    respond_with(@results) do |format|
      format.json { render :json => @results, :methods => [:capacity, :used], :except => [:present_capacity, :dfs_used] }
    end
  end

  private
  def hdfs_stat_select(properties)
    hdfs = Hdfs.find params[:id]
    properties = [:id, :created_at] + properties

    if params[:stat_id]
      where_clause = "id > #{params[:stat_id]}"
    else
      where_clause = "created_at >= '#{params[:stat_min].to_i.minute.ago}'"
      where_clause += " and created_at < '#{params[:stat_max].to_i.minute.ago}'" if params[:stat_max]
    end

    stat_number = hdfs.hdfs_stats.where(where_clause).select("count(*) as count")
    if stat_number.count >= 700
      remove_number = (stat_number.count / 700).floor
      where_clause += " and hdfs_stats.id % #{remove_number} = 0"
    end

    return hdfs.hdfs_stats.where(where_clause).select(properties)
  end
end
