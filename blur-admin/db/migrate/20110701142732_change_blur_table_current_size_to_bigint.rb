class ChangeBlurTableCurrentSizeToBigint < ActiveRecord::Migration
  def self.up
    change_table :blur_tables do |t|
      t.change :current_size, :integer, :limit => 8
    end
  end

  def self.down
    change_table :blur_tables do |t|
      t.change :current_size, :integer
    end
  end
end
