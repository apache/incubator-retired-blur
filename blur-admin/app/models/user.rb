class User < ActiveRecord::Base
  attr_accessible :username, :email, :password, :password_confirmation
  acts_as_authentic

  include RoleModel

  roles_attribute :roles_mask

  # declare the valid roles -- do not change the order if you add more
  # roles later, always append them at the end!
  roles :editor, :admin
end
