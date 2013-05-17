class AddCountColumnToBlurTables < ActiveRecord::Migration
  def self.up
    add_column :blur_tables, :record_count, :integer, :limit => 8
  end

  def self.down
    remove_column :blur_tables, :record_count
  end
end
