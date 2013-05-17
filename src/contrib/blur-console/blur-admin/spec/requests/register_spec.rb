require 'spec_helper'

describe "register" do
  let(:user) { Factory.build :user }
  before do
    visit new_user_path
  end

  context "with valid user parameters" do
    before do
      fill_in 'user_username',              :with => user.username
      fill_in 'user_email',                 :with => user.email
      fill_in 'user_password',              :with => user.password
      fill_in 'user_password_confirmation', :with => user.password_confirmation
      click_button 'Register'
    end

    it "should redirect to the user page" do
      current_path.should == user_path(User.find_by_username user.username)
    end
  end

  context "with invalid user parameters" do
    before do
      fill_in 'user_username', :with => "^%" 
      fill_in 'user_email', :with => 'a'
      fill_in 'user_password', :with => 'b'
      fill_in 'user_password_confirmation', :with => 'c'
      click_button 'Register'
    end

    it "should show the new user page with appropriate errors" do
      current_path.should == users_path
      page.should have_selector '#error_explanation'
      page.should have_content 'Username is too short'
      page.should have_content 'Username should use only letters'
      page.should have_content 'Password is too short'
      page.should have_content 'Password confirmation is too short'
      page.should have_content 'Password doesn\'t match confirmation'
      page.should have_content 'Email should look like an email address.'
    end
  end

  context "with existing user parameters" do
    before do
      user.save
      fill_in 'user_username',              :with => user.username
      fill_in 'user_email',                 :with => user.email
      fill_in 'user_password',              :with => user.password
      fill_in 'user_password_confirmation', :with => user.password_confirmation
      click_button 'Register'
    end

    it "should show the new user page with appropriate errors" do
      current_path.should == users_path
      page.should have_selector '#error_explanation'
      page.should have_content 'Username has already been taken'
      page.should have_content 'Email has already been taken'
    end
  end
end
