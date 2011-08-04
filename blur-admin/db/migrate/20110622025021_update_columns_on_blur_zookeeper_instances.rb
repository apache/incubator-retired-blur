class UpdateColumnsOnBlurZookeeperInstances < ActiveRecord::Migration
  def self.up
    add_column :blur_zookeeper_instances, :name, :string

    remove_column :blur_zookeeper_instances, :port
    remove_column :blur_zookeeper_instances, :created_at
    remove_column :blur_zookeeper_instances, :updated_at
    
    rename_column :blur_zookeeper_instances, :host, :url
  end

  def self.down
    remove_column :blur_zookeeper_instances, :name
    
    add_column :blur_zookeeper_instances, :port, :string
    add_column :blur_zookeeper_instances, :created_at, :datetime
    add_column :blur_zookeeper_instances, :updated_at, :datetime
    
    rename_column :blur_zookeeper_instances, :url, :host
  end
end
