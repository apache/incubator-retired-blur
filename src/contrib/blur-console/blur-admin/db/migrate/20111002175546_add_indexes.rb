class AddIndexes < ActiveRecord::Migration
  def self.up
    add_index :blur_queries, :blur_table_id
    add_index :blur_tables, :cluster_id
    add_index :clusters, :zookeeper_id
    add_index :controllers, :zookeeper_id
    add_index :hdfs_stats, :hdfs_id
    add_index :preferences, :user_id
    add_index :searches, :blur_table_id
    add_index :searches, :user_id
    add_index :shards, :cluster_id
    add_index :system_metrics, :transaction_id
    add_index :system_metrics, :request_id
    add_index :system_metrics, :parent_id
  end

  def self.down
    remove_index :blur_queries, :blur_table_id
    remove_index :blur_tables, :cluster_id
    remove_index :clusters, :zookeeper_id
    remove_index :controllers, :zookeeper_id
    remove_index :hdfs_stats, :hdfs_id
    remove_index :preferences, :user_id
    remove_index :searches, :blur_table_id
    remove_index :searches, :user_id
    remove_index :shards, :cluster_id
    remove_index :system_metrics, :transaction_id
    remove_index :system_metrics, :request_id
    remove_index :system_metrics, :parent_id
  end
end
