class AddHostAndPortToZookeeper < ActiveRecord::Migration
  def self.up
    add_column :zookeepers, :host, :string
    add_column :zookeepers, :port, :string
  end

  def self.down
    remove_column :zookeepers, :host
    remove_column :zookeepers, :port
  end
end
