class Preference < ActiveRecord::Base
  belongs_to :user
  validates :pref_type, :uniqueness => {:scope => :user_id},
                        :inclusion  => {:in => %w(filter column)},
                        :presence => true
  serialize :value
end
