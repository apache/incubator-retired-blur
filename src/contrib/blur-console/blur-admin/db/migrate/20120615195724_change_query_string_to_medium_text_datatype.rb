class ChangeQueryStringToMediumTextDatatype < ActiveRecord::Migration
  def up
    change_column :blur_queries, :query_string, :text, :limit=>16777215
  end

  def down
    change_column :blur_queries, :query_string, :text
  end
end
