class UpdateLicenseTable < ActiveRecord::Migration
  def self.up
    add_column :licenses, :cluster_overage, :integer
  end

  def self.down
    remove_column :licenses, :cluster_overage
  end
end
