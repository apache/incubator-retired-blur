class AddRecordOnlyToSearch < ActiveRecord::Migration
  def self.up
    add_column :searches, :record_only, :boolean, :default => 0
  end

  def self.down
    remove_column :searches, :record_only
  end
end
