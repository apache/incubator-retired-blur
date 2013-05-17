class AddAdminConifgurationsTable < ActiveRecord::Migration
  def change
    create_table :admin_settings do |table|
      table.column :setting, :string, :null => false
      table.column :value, :string
      table.timestamps
    end
  end
end
