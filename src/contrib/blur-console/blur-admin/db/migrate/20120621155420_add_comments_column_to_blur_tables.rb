class AddCommentsColumnToBlurTables < ActiveRecord::Migration
  def self.up
    add_column :blur_tables, :comments, :string
  end
  
  def self.down
    remove_column :blur_tables, :comments
  end

end
