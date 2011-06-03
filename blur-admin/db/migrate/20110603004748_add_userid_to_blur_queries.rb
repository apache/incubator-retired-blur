class AddUseridToBlurQueries < ActiveRecord::Migration
  def self.up
    add_column :blur_queries, :userid, :string
  end

  def self.down
    remove_column :blur_queries, :userid
  end
end
