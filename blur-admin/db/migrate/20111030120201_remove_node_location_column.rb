class RemoveNodeLocationColumn < ActiveRecord::Migration
  def self.up
    remove_column :shards, :node_location
    remove_column :controllers, :node_location
  end

  def self.down
    add_column :shards, :node_location, :string
    add_column :controllers, :node_location, :string
  end
end
