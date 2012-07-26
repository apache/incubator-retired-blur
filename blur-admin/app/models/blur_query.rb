require 'blur_thrift_client'

class BlurQuery < ActiveRecord::Base
  include ActionView::Helpers::NumberHelper
  belongs_to :blur_table
  has_one :cluster, :through => :blur_table
  has_one :zookeeper, :through => :cluster

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

  def summary(user)
    summary_hash =
    {
      :id => id,
      :can_update => user.can?(:update, :blur_queries),
      :userid => print_value(userid),
      :query => print_value(query_string),
      :tablename => print_value(blur_table.table_name),
      :start => print_value(start, 0),
      :time => created_at.getlocal.strftime('%r'),
      :status => summary_state,
      :state => state_str
    }
    summary_hash.delete(:query) if user.cannot?(:index, :blur_queries, :query_string)
    summary_hash
  end

  private

  def summary_state
    if state == 0
      formattedNumber = "%01d" % (100 * complete)
      formattedNumber + '%'
    elsif state == 1
      "(Interrupted) - #{number_to_percentage(100 * complete, :precision => 0)}"
    else
      "Complete"
    end
  end

  def print_value(conditional, default_message = "Not Available")
    return default_message unless conditional
    return conditional unless block_given?
    yield
  end
end
