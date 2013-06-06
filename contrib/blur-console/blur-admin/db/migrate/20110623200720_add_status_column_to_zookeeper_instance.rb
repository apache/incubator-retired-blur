class AddStatusColumnToZookeeperInstance < ActiveRecord::Migration
  def self.up
    add_column :blur_zookeeper_instances, :status, :integer
  end

  def self.down
    remove_column :blur_zookeeper_instances, :status
  end
end
