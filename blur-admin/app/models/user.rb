class User < ActiveRecord::Base
  attr_accessible :username, :email, :password, :password_confirmation, :admin, :editor
  acts_as_authentic

  include RoleModel

  roles_attribute :roles_mask

  # declare the valid roles -- do not change the order if you add more
  # roles later, always append them at the end!
  roles :editor, :admin

  # 'admin' and 'editor' are virtual attributes needed to use form helpers
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

  def admin
    return true if self.has_role? :admin
    false
  end

  def editor
    return true if self.has_role? :editor
    false
  end
end
