class ChangeClusterToBelongToInstance < ActiveRecord::Migration
  def self.up
    rename_column :clusters, :controller_id, :blur_zookeeper_instance_id
  end

  def self.down
    rename_column :clusters, :blur_zookeeper_instance_id, :controller_id
  end
end
