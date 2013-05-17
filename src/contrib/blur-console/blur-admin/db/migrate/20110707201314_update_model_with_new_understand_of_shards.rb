class UpdateModelWithNewUnderstandOfShards < ActiveRecord::Migration
  def self.up
    add_column :blur_tables, :cluster_id, :integer

    drop_table :blur_tables_shards
  end

  def self.down
    remove_column :blur_tables, :cluster_id

    create_table :blur_tables_shards, :id => false do |t|
      t.integer :shard_id
      t.integer :blur_table_id
    end
  end
end
