class ChangeShardtoBlurShards < ActiveRecord::Migration
  def change
    rename_table :shards, :blur_shards
  end
end
