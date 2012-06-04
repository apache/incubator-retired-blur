class AddRecordOnlyToBlurQueries < ActiveRecord::Migration
  def self.up
    add_column :blur_queries, :record_only, :boolean
  end

  def self.down
    remove_column :blur_queries, :record_only
  end
end
