class ChangeColumnstoTextobject < ActiveRecord::Migration
  def self.up
    change_column :searches, :columns, :text
  end

  def self.down
    change_column :searches, :columns, :string
  end
end
