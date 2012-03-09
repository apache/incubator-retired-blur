class AddSafeMode < ActiveRecord::Migration
  def up
    add_column :clusters, :safe_mode, :boolean
  end

  def down
    remove_column :clusters, :safe_mode
  end
end
