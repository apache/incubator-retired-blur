class AddColumnsToHdfsStats < ActiveRecord::Migration
  def self.up
    add_column :hdfs_stats, :host, :string
    add_column :hdfs_stats, :port, :integer
  end

  def self.down
    remove_column :hdfs_stats, :host
    remove_column :hdfs_stats, :port
  end
end
