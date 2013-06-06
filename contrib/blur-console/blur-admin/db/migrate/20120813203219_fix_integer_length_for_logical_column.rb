class FixIntegerLengthForLogicalColumn < ActiveRecord::Migration
  def change
    change_table :hdfs_stats do |t|
      t.change :dfs_used_logical, :integer, :limit => 8
    end
  end
end