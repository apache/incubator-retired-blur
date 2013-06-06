class ChangeLicensesToAddNodeInfo < ActiveRecord::Migration
  def self.up
    add_column :licenses, :node_overage, :int
    add_column :licenses, :grace_period_days_remain, :int
  end

  def self.down
    remove_column :licenses, :node_overage
    remove_column :licenses, :grace_period_days_remain
  end
end
