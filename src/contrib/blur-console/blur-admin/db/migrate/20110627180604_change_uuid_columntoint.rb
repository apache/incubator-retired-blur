class ChangeUuidColumntoint < ActiveRecord::Migration
  def self.up
    change_column :blur_queries, :uuid, :integer
  end

  def self.down
    change_column :blur_queries, :uuid, :string
  end
end
