class RenameBlurZookeeperInstancesToZookeepers < ActiveRecord::Migration
  def self.up
    rename_table :blur_zookeeper_instances, :zookeepers
  end

  def self.down
    rename_table :zookeepers, :blur_zookeeper_instances
  end
end
