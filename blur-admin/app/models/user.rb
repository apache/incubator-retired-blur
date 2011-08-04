class User < ActiveRecord::Base
  attr_accessible :username, :email, :password, :password_confirmation,
                  :admin, :editor, :auditor, :reader, :searcher
  acts_as_authentic

  has_many :searches
  has_many :preferences

  include RoleModel

  roles_attribute :roles_mask

  # declare the valid roles -- do not change the order if you add more
  # roles later, always append them at the end!
  roles :editor, :admin, :reader, :auditor, :searcher

  #returns the array of saved cols
  def column_preference
    Preference.find_or_create_by_user_id_and_pref_type(self.id, 'column') do |preference|
      preference.name = 'column'
      preference.value = []
    end
  end

  #returns the array of saved cols
  def filter_preference
    Preference.find_or_create_by_user_id_and_pref_type(self.id, 'filter') do |preference|
      preference.name = 'filter'
      preference.value = {}
    end
  end

  # the roles are virtual attributes needed to use form helpers
  User.valid_roles.each do |role|
    define_method role do
      return true if self.has_role? role
      false
    end

    define_method role.to_s+"=" do |flag|
      self.roles << role     if flag == '1' and !self.has_role? role
      self.roles.delete role if flag == '0' and  self.has_role? role
    end
  end
end
