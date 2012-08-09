class ChangeAuditIntegersToStrings < ActiveRecord::Migration
  def up
    change_column :audits, :mutation, :string
    change_column :audits, :model_affected, :string
  end

  def down
    change_column :audits, :mutation, :integer
    change_column :audits, :model_affected, :integer
  end
end
