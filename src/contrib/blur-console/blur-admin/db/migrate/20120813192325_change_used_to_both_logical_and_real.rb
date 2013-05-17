class ChangeUsedToBothLogicalAndReal < ActiveRecord::Migration
  def change
    change_table :hdfs_stats do |t|
      t.rename :dfs_used, :dfs_used_real
      t.integer :dfs_used_logical
    end
  end
end
