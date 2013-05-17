class AddPostFilterToSearch < ActiveRecord::Migration
  def change
    add_column :searches, :post_filter, :text

  end
end
