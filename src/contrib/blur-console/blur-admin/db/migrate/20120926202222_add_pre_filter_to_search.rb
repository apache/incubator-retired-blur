class AddPreFilterToSearch < ActiveRecord::Migration
  def change
    add_column :searches, :pre_filter, :text

  end
end
