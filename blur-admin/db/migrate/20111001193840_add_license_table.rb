class AddLicenseTable < ActiveRecord::Migration
  def self.up
    create_table :licenses, {:id => false} do |t|
      t.string :org
      t.date :issued_date
      t.date :expires_date
    end
  end

  def self.down
    drop_table :licenses
  end
end
