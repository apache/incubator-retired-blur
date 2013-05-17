class ChangeBlurQueriesModel < ActiveRecord::Migration
  def self.up
    add_column :blur_queries, :times, :string
    remove_column :blur_queries, :cpu_time
    remove_column :blur_queries, :real_time

    remove_column :blur_queries, :interrupted
    remove_column :blur_queries, :running

    rename_column :blur_queries, :complete, :complete_shards
    add_column :blur_queries, :total_shards, :integer

    add_column :blur_queries, :state, :integer

  end

  def self.down
    remove_column :blur_queries, :times
    add_column :blur_queries, :cpu_time, :integer
    add_column :blur_queries, :real_time, :integer

    add_column :blur_queries, :interrupted, :boolean
    add_column :blur_queries, :running, :boolean

    rename_column :blur_queries, :complete_shards, :complete
    remove_column :blur_queries, :total_shards

    remove_column :blur_queries, :state
  end
end
