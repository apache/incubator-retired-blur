class CreateBlurTables < ActiveRecord::Migration
  def self.up
    create_table :blur_tables do |t|
      t.string :table_name
      t.integer :current_size
      t.integer :query_usage

      t.timestamps
    end
  end

  def self.down
    drop_table :blur_tables
  end
end
