class RenameStatusColumns < ActiveRecord::Migration
  def up
    rename_column :blur_controllers, :status, :controller_status
    rename_column :blur_shards, :status, :shard_status
    rename_column :blur_tables, :status, :table_status
    rename_column :zookeepers, :status, :zookeeper_status
  end

  def down
    rename_column :blur_controllers, :controller_status, :status
    rename_column :blur_shards, :shard_status, :status
    rename_column :blur_tables, :table_status, :status
    rename_column :zookeepers, :status, :zookeeper_status
  end
end
