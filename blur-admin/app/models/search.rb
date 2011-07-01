class Search < ActiveRecord::Base
  belongs_to :blur_table
  belongs_to :user
end
