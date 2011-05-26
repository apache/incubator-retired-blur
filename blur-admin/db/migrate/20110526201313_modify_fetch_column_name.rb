class ModifyFetchColumnName < ActiveRecord::Migration
  def self.up
    rename_column :blur_queries, :fetch, :fetch_num
  end

  def self.down
    rename_column :blur_queries, :fetch_num, :fetch
  end
end
