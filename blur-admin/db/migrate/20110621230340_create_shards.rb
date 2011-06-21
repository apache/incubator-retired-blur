class CreateShards < ActiveRecord::Migration
  def self.up
    create_table :shards do |t|
      t.integer :status
      t.string :blur_version
      t.string :node_name
      t.string :node_location
      t.integer :cluster_id

      t.timestamps
    end
  end

  def self.down
    drop_table :shards
  end
end
