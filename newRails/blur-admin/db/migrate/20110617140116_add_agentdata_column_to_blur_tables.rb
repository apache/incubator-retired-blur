class AddAgentdataColumnToBlurTables < ActiveRecord::Migration
  def self.up
    add_column :blur_tables, :table_uri, :boolean
    add_column :blur_tables, :table_analyzer, :text
    add_column :blur_tables, :schema, :text
    add_column :blur_tables, :server, :text
  end

  def self.down
    remove_column :blur_tables, :table_uri
    remove_column :blur_tables, :table_analyzer
    remove_column :blur_tables, :schema
    remove_column :blur_tables, :server
  end
end
