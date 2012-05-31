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
    table_query_info = self.recent_queries
    serial_properties = super(options)
    serial_properties.delete('server')
    serial_properties.delete('table_schema')
    serial_properties[:queried_recently] = table_query_info['queried_recently']
    serial_properties[:hosts] = self.hosts
    serial_properties[:schema] = self.schema
    serial_properties[:sparkline] = table_query_info['sparkline']
    serial_properties[:average_queries] = table_query_info['average_queries']
    serial_properties
  end

  # Returns a map of host => [shards] of all hosts/shards associated with the table
  def hosts
    JSON.parse read_attribute(:server)
  end

  def schema
    if !self.table_schema.blank?
      # sort columns, and then sort column families
      if block_given?
        (JSON.parse self.table_schema).each{|n| n['columns'].sort_by!{|k| k['name']}}.sort &Proc.new
      else
        (JSON.parse self.table_schema).each{|n| n['columns'].sort_by!{|k| k['name']}}.sort_by{|k| k['name']}
      end
    else
      return nil
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
    puts
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
    queries = self.blur_queries.select("minute(created_at) as minute, count(created_at) as cnt").where("created_at > '#{10.minutes.ago}'").group("minute(created_at)").order("created_at")
    sparkline = [[0,0],[1,0],[2,0],[3,0],[4,0],[5,0],[6,0],[7,0],[8,0],[9,0]] #default format for sparkline
    time = 10.minutes.ago.min
    average_queries = 0.0
    queried_recently = false

    queries.each do |row|
      min = row.minute
      cnt = row.cnt

      if min >= time
        min = min - time
      else
        min = 60 - time + min
      end

      sparkline[min] = [min, cnt]
      average_queries += cnt
      if !queried_recently && min >= 5
        queried_recently = true
      end
    end
    average_queries = average_queries / 10.0

    recent_queries = Hash['sparkline' => sparkline, 'average_queries' => average_queries, 'queried_recently' => queried_recently]
  end

end
