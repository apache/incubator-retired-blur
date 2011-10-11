class CreateControllers < ActiveRecord::Migration
  def self.up
    create_table :controllers do |t|
      t.integer :status
      t.string :blur_version
      t.string :node_name
      t.string :node_location
      t.integer :blur_zookeeper_instance_id

      t.timestamps
    end
  end

  def self.down
    drop_table :controllers
  end
end
