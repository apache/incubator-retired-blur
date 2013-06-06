class CreateBlurQueries < ActiveRecord::Migration
  def self.up
    create_table :blur_queries do |t|
      t.string :query_string
      t.integer :cpu_time
      t.integer :real_time
      t.integer :complete
      t.boolean :interrupted
      t.boolean :running
      t.string :uuid

      t.timestamps
    end
  end

  def self.down
    drop_table :blur_queries
  end
end
