class AddColumnShardIdToBlurTables < ActiveRecord::Migration
  def self.up
    add_column :blur_tables, :shard_id, :integer
  end

  def self.down
    remove_column :blur_tables, :shard_id
  end
end
