class ChangeHostPortToUrl < ActiveRecord::Migration
  def self.up
    remove_columns(:zookeepers, :host, :port)
    add_column(:zookeepers, :blur_urls, :string, :limit=>4000)
  end

  def self.down
    add_column :zookeepers, :host, :string
    add_column :zookeepers, :port, :string
    remove_column :zookeepers, :blur_urls
  end
end
