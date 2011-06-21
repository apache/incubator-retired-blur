class Cluster < ActiveRecord::Base
  belongs_to :controller
  has_many :shards
end
