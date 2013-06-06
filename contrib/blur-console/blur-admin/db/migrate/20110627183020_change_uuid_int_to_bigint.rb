class ChangeUuidIntToBigint < ActiveRecord::Migration
  def self.up
    change_column :blur_queries, :uuid, :integer, :limit => 8
  end

  def self.down
    change_column :blur_queries, :uuid, :integer
  end
end
