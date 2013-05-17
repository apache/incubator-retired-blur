class AddZookeeperToAudits < ActiveRecord::Migration
  def change
    add_column :audits, :zookeeper_affected, :string
  end
end
