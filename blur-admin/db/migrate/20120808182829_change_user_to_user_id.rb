class ChangeUserToUserId < ActiveRecord::Migration
  def up
    change_column :audits, :user, :integer
    rename_column :audits, :user, :user_id
  end

  def down
    rename_column :audits, :user_id, :user
    change_column :audits, :user, :string
  end
end
