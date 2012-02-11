class AddingUseridtoTheModelForAssociation < ActiveRecord::Migration
  def self.up
    add_column :preferences, :user_id, :integer
  end

  def self.down
    remove_column :preferences, :user_id
  end
end
