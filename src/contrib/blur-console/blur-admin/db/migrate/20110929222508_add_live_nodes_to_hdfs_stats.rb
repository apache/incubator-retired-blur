class AddLiveNodesToHdfsStats < ActiveRecord::Migration
  def self.up
    add_column :hdfs_stats, :live_nodes, :int
  end

  def self.down
    remove_column :hdfs_stats, :live_nodes
  end
end
