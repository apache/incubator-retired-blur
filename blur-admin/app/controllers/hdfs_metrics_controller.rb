class HdfsMetricsController < ApplicationController
  def index
    @hdfs_index = Hdfs.all
  end

  def disk_cap_usage
    hdfs_stat_select [:present_capacity, :dfs_used]
  end

  def live_dead_nodes
    hdfs_stat_select [:under_replicated, :corrupt_blocks]
  end

  def block_info
    hdfs_stat_select [:live_nodes, :dead_nodes]
  end

  private
  def hdfs_stat_select(properties)
    hdfs = Hdfs.find params[:id]
    properties = [:id, :created_at] + properties
    render :json => hdfs.hdfs_stats.where('id > ?', params[:stat_id]).select(properties) if params[:stat_id]
    render :json => hdfs.hdfs_stats.where('created_at > ?', (params[:stat_days] || 1).day.ago).select(properties)
  end
end
