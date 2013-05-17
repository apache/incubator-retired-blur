class ChangeTypeToSmallerSizeMedText < ActiveRecord::Migration
  def up
    change_column :blur_queries, :query_string, :text, :limit => 64.kilobytes + 1
  end

  def down
    change_column :blur_queries, :query_string, :text, :limit => 16777215
  end
end
