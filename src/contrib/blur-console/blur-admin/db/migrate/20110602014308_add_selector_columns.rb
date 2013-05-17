class AddSelectorColumns < ActiveRecord::Migration
  def self.up
    add_column :blur_queries, :selector_column_families, :text
    add_column :blur_queries, :selector_columns, :text

    remove_column :blur_queries, :selectors
  end

  def self.down
    remove_column :blur_queries, :selector_column_families
    remove_column :blur_queries, :selector_columns

    add_column :blur_queries, :selectors, :string
  end
end
