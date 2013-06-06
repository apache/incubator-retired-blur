class AddTimestampsToBlurObjects < ActiveRecord::Migration
  def change
    change_table :blur_shards do |table|
      table.timestamps
    end
    change_table :blur_controllers do |table|
      table.timestamps
    end
  end
end
