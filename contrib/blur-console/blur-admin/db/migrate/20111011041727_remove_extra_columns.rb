class RemoveExtraColumns < ActiveRecord::Migration
  def self.up
    remove_column :shards, :created_at
    remove_column :shards, :updated_at
    remove_column :hdfs_stats, :updated_at
    remove_column :hdfs, :created_at
    remove_column :hdfs, :updated_at
    remove_column :controllers, :created_at
    remove_column :controllers, :updated_at
    remove_column :clusters, :created_at
    remove_column :clusters, :updated_at
    remove_column :blur_tables, :created_at
  end

  def self.down
    add_column :shards, :created_at, :datetime
    add_column :shards, :updated_at, :datetime
    add_column :hdfs_stats, :updated_at, :datetime
    add_column :hdfs, :created_at, :datetime
    add_column :hdfs, :updated_at, :datetime
    add_column :controllers, :created_at, :datetime
    add_column :controllers, :updated_at, :datetime
    add_column :clusters, :created_at, :datetime
    add_column :clusters, :updated_at, :datetime
    add_column :blur_tables, :created_at, :datetime
  end
end
