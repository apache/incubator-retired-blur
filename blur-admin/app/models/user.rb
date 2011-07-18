class User < ActiveRecord::Base
  attr_accessible :username, :email, :password, :password_confirmation, :admin, :editor, :auditor, :reader
  acts_as_authentic

  has_many :searches
  has_many :preferences

  include RoleModel

  roles_attribute :roles_mask

  # declare the valid roles -- do not change the order if you add more
  # roles later, always append them at the end!
  roles :editor, :admin, :reader, :auditor
  
  #returns the array of saved cols
  def saved_cols
    ret = []
    self.preferences.find_all_by_pref_type(:column).each do |pref|
      ret = JSON.parse(pref[:value]) unless pref[:value] == "null"
    end
    ret.flatten.uniq
  end

  # the roles are virtual attributes needed to use form helpers
  def admin=(admin)
    if admin == "1"
      self.roles << :admin unless self.has_role? :admin
    elsif admin == "0"
      self.roles.delete :admin if self.has_role? :admin
    end
  end

  def editor=(editor)
    if editor == "1"
      self.roles << :editor unless self.has_role? :editor
    elsif editor == "0"
      self.roles.delete :editor if self.has_role? :editor
    end
  end

  def reader=(reader)
    if reader == "1"
      self.roles << :reader unless self.has_role? :reader
    elsif reader == "0"
      self.roles.delete :reader if self.has_role? :reader
    end
  end

  def auditor=(auditor)
    if auditor == "1"
      self.roles << :auditor unless self.has_role? :auditor
    elsif auditor == "0"
      self.roles.delete :auditor if self.has_role? :auditor
    end
  end

  def admin
    return true if self.has_role? :admin
    false
  end

  def editor
    return true if self.has_role? :editor
    false
  end
end

def reader
  return true if self.has_role? :reader
  false
end

def auditor
  return true if self.has_role? :auditor
  false
end
