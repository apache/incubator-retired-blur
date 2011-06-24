class RemoveTableNameColumnFromBlurQueries < ActiveRecord::Migration
  def self.up
    remove_column :blur_queries, :table_name
  end

  def self.down
    add_column :blur_queries, :table_name, :string
  end
end
