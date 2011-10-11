class CreateSearches < ActiveRecord::Migration
  def self.up
    create_table :searches do |t|
      t.boolean :super_query
      t.string :columns
      t.integer :fetch
      t.integer :offset
      t.string :name
      t.string :query
      t.integer :blur_table_id
      t.integer :user_id

      t.timestamps
    end
  end

  def self.down
    drop_table :searches
  end
end
