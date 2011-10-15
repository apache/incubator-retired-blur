class Preference < ActiveRecord::Base
  belongs_to :user
  validates :pref_type, :uniqueness => {:scope => :user_id},
                        :inclusion  => {:in => %w(filter column)},
                        :presence => true
  serialize :value
  
  def value
    @value || ""
  end

  # Scopes allow you to call user.preferences.column
  # or user.preferences.filter
  scope :column, where(:pref_type => 'column')
  scope :filter, where(:pref_type => 'filter')
end
