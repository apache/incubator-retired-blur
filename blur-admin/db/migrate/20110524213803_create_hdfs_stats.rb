class CreateHdfsStats < ActiveRecord::Migration
  def self.up
    create_table :hdfs_stats do |t|
      t.string :hdfs_name
      t.integer :config_capacity
      t.integer :present_capacity
      t.integer :dfs_remaining
      t.integer :dfs_used
      t.decimal :dfs_used_percent
      t.integer :under_replicated
      t.integer :corrupt_blocks
      t.integer :missing_blocks
      t.integer :total_nodes
      t.integer :dead_nodes

      t.timestamps
    end
  end

  def self.down
    drop_table :hdfs_stats
  end
end
