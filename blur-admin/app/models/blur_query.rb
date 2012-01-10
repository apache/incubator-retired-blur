require 'blur_thrift_client'

class BlurQuery < ActiveRecord::Base
  include ActionView::Helpers::NumberHelper
  belongs_to :blur_table
  has_one :cluster, :through => :blur_table

  def cancel
    begin
      BlurThriftClient.client(blur_table.zookeeper.blur_urls).cancelQuery self.blur_table.table_name, self.uuid
      return true
    rescue Exception => e
      logger.error "Exception in BlurQueries.cancel:"
      logger.error e
      return false
    end
  end

  # rails 3.0 does not allow nested has_one :through relationships
  def zookeeper
    self.blur_table.zookeeper
  end

  def times
    JSON.parse(read_attribute(:times)).each do |shard, value|
      value.reject! {|type, value| type == 'setCpuTime' or type == 'setRealTime'}
    end
  end

  def state_str
    case read_attribute(:state)
      when 0 then "Running"
      when 1 then "Interrupted"
      when 2 then "Complete"
      else nil
    end
  end

  def complete
    if self.total_shards == 0
      0
    else
      self.complete_shards / self.total_shards.to_f
    end
  end
  
  def self.where_zookeeper(zookeeper_id)
    joins(:blur_table => :cluster).where(:blur_table =>{:clusters => {:zookeeper_id => zookeeper_id}}).includes(:blur_table).order("created_at DESC")
  end
  
  def self.filter_on_time_range(range)
    where(:updated_at => range)
  end

  def summary(user)
    if user.can?(:index, :blur_queries, :query_string)
      [id, user.can?(:update, :blur_queries), userid, query_string, blur_table.table_name, start, fetch_num, summary_state]
    else
      [id, user.can?(:update, :blur_queries), userid, blur_table.table_name, start, fetch_num, summary_state]
    end
  end

  private

  def summary_state
    if state == 0
      number_to_percentage(100 * complete, :precision => 0)
    elsif state == 1
      "#{number_to_percentage(100 * complete, :precision => 0)} (Interrupted)"
    else
      "Complete"
    end
  end
end
