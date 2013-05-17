class DropLicensesTable < ActiveRecord::Migration
  def up
    drop_table :licenses
  end

  def down
    create_table "licenses", :id => false, :force => true do |t|
      t.string  "org"
      t.date    "issued_date"
      t.date    "expires_date"
      t.integer "node_overage"
      t.integer "grace_period_days_remain"
      t.integer "cluster_overage"
    end
  end
end
