class BlurTable < ActiveRecord::Base
  require 'blur_thrift_client'

  belongs_to :cluster
  has_many :blur_queries, :dependent => :destroy
  has_many :searches, :dependent => :destroy
  has_one :zookeeper, :through => :cluster

  scope :deleted, where("status=?", 0)
  scope :disabled, where("status=?", 2)
  scope :active, where("status=?", 4)

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

  def has_queried_recently?
    self.blur_queries.where("created_at > '#{5.minutes.ago}'").count > 0
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
    return BlurThriftClient.client(blur_urls).terms self.table_name, family, column, startWith, size
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
      return true;
    rescue
      return false;
    end
  end
end
