require 'spec_helper'

describe 'User Model' do

  before do
    @user = User.new( :username              => 'bob', 
                      :email                 => 'bob@example.com',
                      :password              => 'password',
                      :password_confirmation => 'password')
  end

  describe 'create a user' do
    it 'is valid with valid parameters' do
      @user.should be_valid
    end

    it 'is invalid with no username' do
      @user.username = nil
      @user.should_not be_valid
    end

    it 'is invalid with no password' do
      @user.password = nil
      @user.password_confirmation = nil
      @user.should_not be_valid
    end
    
    it 'is invalid with no email' do
      @user.email = nil
      @user.should_not be_valid
    end

    it 'is invalid with invalid email' do
      @user.email = 'invalid'
      @user.should_not be_valid
      @user.email = 'invalid@'
      @user.should_not be_valid
    end
    
    
    it 'is invalid with duplicate username' do
      @user.save
      duplicate_user = User.new(:username              => @user.username,
                                :email                 => 'dup_user@example.com',
                                :password              => 'dup_password', 
                                :password_confirmation => 'dup_password')
      duplicate_user.should_not be_valid
    end

    it 'is invalid with duplicate email'  do
      @user.save
      duplicate_user = User.new(:username              => 'duplicate_user',
                                :email                 => @user.email,
                                :password              => 'dup_password', 
                                :password_confirmation => 'dup_password')
      duplicate_user.should_not be_valid
    end

    it 'is valid with long username' do
      username = ''
      100.times {username += 'a'}
      @user.username = username
      @user.should be_valid
    end
    
    it 'is valid with email missing dot ending' do
      @user.email = "tester@test"
      @user.should be_valid
    end

    it 'is invalid with short password' do
      password = 'abc'
      @user.password = password
      @user.password_confirmation = password
      @user.should_not be_valid
    end

    it 'is valid with non alpha-numeric characters in password' do
      password = "aA1!,.:' "
      @user.password = password
      @user.password_confirmation = password
      @user.should be_valid
    end
  end

  describe 'ability' do
    it 'should create a new ability when an ability isnt cached' do
      pending
    end
  end
end

