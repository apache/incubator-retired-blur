class BlurTable < ActiveRecord::Base
  require 'blur_thrift_client'

  belongs_to :cluster
  has_many :blur_queries, :dependent => :destroy
  has_many :searches, :dependent => :destroy
  has_one :zookeeper, :through => :cluster

  scope :deleted, where("status=?", 0)
  scope :disabled, where("status=?", 2)
  scope :active, where("status=?", 4)

  def as_json(options={})
    serial_properties = super(options)
    serial_properties.delete('server')
    serial_properties.delete('table_schema')
    serial_properties.delete('updated_at')
    serial_properties['queried_recently'] = self.recent_queries

    host_count = self.hosts.keys.length
    shard_count = 0
    self.hosts.values.each{ |shards| shard_count += shards.length }

    serial_properties['server_info'] = host_count.to_s + ' | ' + shard_count.to_s
    serial_properties['comments'] = self.comments
    serial_properties
  end

  # Returns a map of host => [shards] of all hosts/shards associated with the table
  def hosts
    read_attribute(:server).blank? ? {} : (JSON.parse read_attribute(:server))
  end

  def schema
    return nil if self.table_schema.blank?
    # sort columns inline
    sorted_schema = (JSON.parse self.table_schema).each{|n| n['columns'].sort_by!{|k| k['name']}}
    if block_given?
      sorted_schema.sort &Proc.new
    else
      # sort column families
      sorted_schema.sort_by{|k| k['name']}
    end
  end

  def record_count
    read_attribute(:record_count).to_s.reverse.gsub(%r{([0-9]{3}(?=([0-9])))}, "\\1#{','}").reverse
  end

  def row_count
    read_attribute(:row_count).to_s.reverse.gsub(%r{([0-9]{3}(?=([0-9])))}, "\\1#{','}").reverse
  end

  def is_enabled?
    self.status == 4
  end

  def is_disabled?
    self.status == 2
  end

  def is_deleted?
    self.status == 0
  end

  def terms(blur_urls,family,column,startWith,size)
    return BlurThriftClient.client(blur_urls).terms(self.table_name, family, column, startWith, size)
  end

  def enable(blur_urls)
    begin
      BlurThriftClient.client(blur_urls).enableTable self.table_name
    ensure
      return self.is_enabled?
    end
  end

  def disable(blur_urls)
    begin
      BlurThriftClient.client(blur_urls).disableTable self.table_name
    ensure
      return self.is_disabled?
    end
  end

  def blur_destroy(underlying=false, blur_urls)
    begin
      BlurThriftClient.client(blur_urls).removeTable self.table_name, underlying
      return true
    rescue
      return false
    end
  end

  def recent_queries
    self.blur_queries
      .select("minute(created_at) as minute, count(*) as cnt")
      .where("created_at > '#{10.minutes.ago}'")
      .count > 0
  end
end
