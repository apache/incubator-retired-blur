class UpdateHdfsStats < ActiveRecord::Migration
  def self.up
    remove_column :hdfs_stats, :hdfs_name
    add_column :hdfs_stats, :hdfs_id, :integer
  end

  def self.down
    add_column :hdfs_stats, :hdfs_name, :string
    remove_column :hdfs_stats, :hdfs_id
  end
end
