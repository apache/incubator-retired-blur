class AddTableIdColumnToBlurQueries < ActiveRecord::Migration
  def self.up
    add_column :blur_queries, :blur_table_id, :int
  end

  def self.down
    remove_column :blur_queries, :blur_table_id
  end
end
