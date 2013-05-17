class AddRowCountToBlurTable < ActiveRecord::Migration
  def self.up
    add_column :blur_tables, :row_count, :integer, :limit => 8
  end

  def self.down
    remove_column :blur_tables, :row_count
  end
end
