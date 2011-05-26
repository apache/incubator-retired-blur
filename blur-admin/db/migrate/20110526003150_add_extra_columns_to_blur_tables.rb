class AddExtraColumnsToBlurTables < ActiveRecord::Migration
  def self.up
    add_column :blur_queries, :table_name, :string
    add_column :blur_queries, :super_query_on, :boolean
    add_column :blur_queries, :facets, :string
    add_column :blur_queries, :selectors, :string
    add_column :blur_queries, :start, :integer
    add_column :blur_queries, :fetch, :integer
  end

  def self.down
    remove_column :blur_queries, :table_name
    remove_column :blur_queries, :super_query_on
    remove_column :blur_queries, :facets
    remove_column :blur_queries, :selectors
    remove_column :blur_queries, :start
    remove_column :blur_queries, :fetch
  end
end
