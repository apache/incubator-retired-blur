class Preference < ActiveRecord::Base
  belongs_to :user
  validates :pref_type, :uniqueness => {:scope => :user_id}, :presence => true
  serialize :value

  # Scopes allow you to call user.preferences.column
  # or user.preferences.filter
  scope :column, where(:pref_type => 'column')
end
