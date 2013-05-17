class ChangeBlurTablesUpdatedAt < ActiveRecord::Migration
  def up
    change_column :blur_tables, :updated_at, :datetime, {:null => true}
  end

  def down
    change_column :blur_tables, :updated_at, :datetime, {:null => false}
  end
end
