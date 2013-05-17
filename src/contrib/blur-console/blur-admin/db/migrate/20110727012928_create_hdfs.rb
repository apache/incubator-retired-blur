class CreateHdfs < ActiveRecord::Migration
  def self.up
    create_table :hdfs do |t|
      t.string :host
      t.string :port
      t.string :name

      t.timestamps
    end
  end

  def self.down
    drop_table :hdfs
  end
end
