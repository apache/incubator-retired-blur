class ChangeSchemaColumn < ActiveRecord::Migration
  def self.up
    rename_column :blur_tables, :schema, :table_schema
  end

  def self.down
    rename_column :blur_tables, :table_schema, :schema
  end
end
