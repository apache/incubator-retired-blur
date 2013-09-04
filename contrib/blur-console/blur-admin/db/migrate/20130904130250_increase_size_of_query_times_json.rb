class IncreaseSizeOfQueryTimesJson < ActiveRecord::Migration
  def up
    change_column :blur_queries, :times, :text
  end

  def down
    change_column :blur_queries, :times, :string
  end
end
