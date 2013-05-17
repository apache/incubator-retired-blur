class IncreaseSizeOfQueryColumn < ActiveRecord::Migration
  def self.up
    change_column :blur_queries, :query_string, :text
  end

  def self.down
    change_column :blur_queries, :query_string, :string
  end
end
