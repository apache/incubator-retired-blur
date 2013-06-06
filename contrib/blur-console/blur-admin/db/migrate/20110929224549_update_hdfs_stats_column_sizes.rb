class UpdateHdfsStatsColumnSizes < ActiveRecord::Migration
  def self.up
    change_column :hdfs_stats, :config_capacity, :integer, :limit => 7
    change_column :hdfs_stats, :present_capacity, :integer, :limit => 7
    change_column :hdfs_stats, :dfs_remaining, :integer, :limit => 7
    change_column :hdfs_stats, :dfs_used, :integer, :limit => 7
    change_column :hdfs_stats, :under_replicated, :integer, :limit => 7
    change_column :hdfs_stats, :corrupt_blocks, :integer, :limit => 7
    change_column :hdfs_stats, :missing_blocks, :integer, :limit => 7
    change_column :hdfs_stats, :port, :string
    change_column :hdfs_stats, :dfs_used_percent, :float
  end

  def self.down
    change_column :hdfs_stats, :config_capacity, :integer
    change_column :hdfs_stats, :present_capacity, :integer
    change_column :hdfs_stats, :dfs_remaining, :integer
    change_column :hdfs_stats, :dfs_used, :integer
    change_column :hdfs_stats, :under_replicated, :integer
    change_column :hdfs_stats, :corrupt_blocks, :integer
    change_column :hdfs_stats, :missing_blocks, :integer
    change_column :hdfs_stats, :port, :integer
    change_column :hdfs_stats, :dfs_used_percent, :decimal
  end
end
