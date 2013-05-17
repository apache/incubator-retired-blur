class FixTableTypes < ActiveRecord::Migration
  def self.up
    change_column :blur_tables, :status, :integer
    change_column :blur_tables, :table_uri, :string
  end

  def self.down
    change_column :blur_tables, :status, :boolean
    change_column :blur_tables, :table_uri, :boolean
  end
end
