class CreateShardsBlurTablesTable < ActiveRecord::Migration
  def self.up
    create_table :blur_tables_shards, :id => false do |t|
      t.integer :shard_id
      t.integer :blur_table_id
    end
    remove_column :blur_tables, :shard_id
  end

  def self.down
    drop_table :blur_tables_shards
    add_column :blur_tables, :shard_id, :integer
  end
end
