class Search < ActiveRecord::Base
  belongs_to :blur_table
  belongs_to :user

  def prepare_search
    bq = Blur::BlurQuery.new :queryStr => self.query, :fetch => self.fetch, :start => self.offset, :uuid => Time.now.to_i*1000+rand(1000)

    if !self.super_query?
      bq.superQueryOn = false
    end
    bq
  end

  def order_columns
    
  end
end
