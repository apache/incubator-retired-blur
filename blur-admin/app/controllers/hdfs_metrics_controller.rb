class HdfsMetricsController < ApplicationController
  def index
    @hdfs_index = Hdfs.all
  end

  def disk_cap_usage
    results = hdfs_stat_select [:present_capacity, :dfs_used]
    render :json => results, :methods => [:capacity, :used], :except => [:present_capacity, :dfs_used]
  end

  def live_dead_nodes
    results = hdfs_stat_select [:live_nodes, :dead_nodes]
    render :json => results
  end

  def block_info
    results = hdfs_stat_select [:under_replicated, :corrupt_blocks]
    render :json => results
  end

  private
  def hdfs_stat_select(properties)
    hdfs = Hdfs.find params[:id]
    properties = [:id, :created_at] + properties
    return hdfs.hdfs_stats.where('id > ?', params[:stat_id]).select(properties) if params[:stat_id]
    return hdfs.hdfs_stats.where('created_at > ?', (params[:stat_days].nil?? 1 : params[:stat_days]).to_i.minute.ago).select(properties)
  end
end
