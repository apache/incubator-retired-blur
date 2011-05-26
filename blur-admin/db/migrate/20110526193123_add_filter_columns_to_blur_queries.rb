class AddFilterColumnsToBlurQueries < ActiveRecord::Migration
  def self.up
    add_column :blur_queries, :pre_filters, :text, :limit => 64.kilobytes + 1
    add_column :blur_queries, :post_filters, :text, :limit => 64.kilobytes + 1
  end

  def self.down
    remove_column :blur_queries, :pre_filters
    remove_column :blur_queries, :post_filters
  end
end
