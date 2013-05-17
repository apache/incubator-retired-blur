class RenameBlurZookeeperInstanceId < ActiveRecord::Migration
  def self.up
    rename_column :clusters, :blur_zookeeper_instance_id, :zookeeper_id
    rename_column :controllers, :blur_zookeeper_instance_id, :zookeeper_id
  end

  def self.down
    rename_column :clusters, :zookeeper_id, :blur_zookeeper_instance_id
    rename_column :controllers, :zookeeper_id, :blur_zookeeper_instance_id
  end
end
