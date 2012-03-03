require 'spec_helper'

describe "login" do
  # generate a valid user
  let(:user) { Factory.create :user }

  before do
    visit login_path
  end

  context "with valid login credentials" do
    before do
      fill_in 'user_session_username', :with => user.username
      fill_in 'user_session_password', :with => user.password
      click_button 'Log In'
    end

    it "should render the dashboard" do
      current_path.should == root_path
    end
  end

  context "with invalid password" do
    before do
      fill_in 'user_session_username', :with => user.username
      fill_in 'user_session_password', :with => 'invalid'
      click_button 'Log In'
    end

    it "should render the new user sessions page with apppropriate errors" do
      current_path.should == user_sessions_path
      page.should have_selector '#error_explanation'
      page.should have_content 'Password is not valid'
    end
  end

  context "with invalid username" do
    before do
      fill_in 'user_session_username', :with => 'invalid'
      fill_in 'user_session_password', :with => user.password
      click_button 'Log In'
    end

    it "should render the new user sessions page with apppropriate errors" do
      current_path.should == user_sessions_path
      page.should have_selector '#error_explanation'
      page.should have_content 'Username is not valid'
    end
  end
  context "with no credentials" do
    before do
      click_button 'Log In'
    end

    it "should render the new user sessions page with apppropriate errors" do
      current_path.should == user_sessions_path
      page.should have_selector '#error_explanation'
      page.should have_content 'You did not provide any details for authentication.'
    end
  end
end
