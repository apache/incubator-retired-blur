class AddStatusColumnToBlurTables < ActiveRecord::Migration
  def self.up
    add_column :blur_tables, :status, :boolean
  end

  def self.down
    remove_column :blur_tables, :record_count
  end
end
