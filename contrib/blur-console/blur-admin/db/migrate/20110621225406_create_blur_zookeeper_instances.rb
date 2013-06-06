class CreateBlurZookeeperInstances < ActiveRecord::Migration
  def self.up
    create_table :blur_zookeeper_instances do |t|
      t.string :host
      t.string :port

      t.timestamps
    end
  end

  def self.down
    drop_table :blur_zookeeper_instances
  end
end
