class ChangeBlurQueriesUuidToString < ActiveRecord::Migration
  def up
    change_column :blur_queries, :uuid, :string
  end

  def down
    change_column :blur_queries, :uuid, :limit => 8
  end
end
