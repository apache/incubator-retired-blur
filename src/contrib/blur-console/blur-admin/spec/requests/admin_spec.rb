require 'spec_helper'

describe "Admin" do
  before(:each) do
    setup_tests
    visit users_path
  end


  context "Page is loaded" do
    it "should display the admin table" do
      page.should have_css('div', :id => 'users_table')
    end
    it "should have the correct table headers" do
      page.should have_content('Username')
      page.should have_content('Name')
      page.should have_content('Email')
      page.should have_content('Roles')
      page.should have_content('Actions')
    end
    it "should display an edit button for the user" do
      find_link('Edit')[:href].should == edit_user_path(@user.id)
    end
    it "should display a button to create a new user" do
      find_link('New User')[:href].should == new_user_path
    end
  end

  context "A user is being edited" do
    before(:each) do
      visit users_path
      click_link 'Edit'
    end
    it "should render the edit user form" do
      current_path.should == edit_user_path(@user.id)
      page.should have_field('user_name')
      page.should have_field('user_email')
      page.should have_field('user_password')
      page.should have_field('user_password_confirmation')

      page.should have_field('user_role_editor')
      page.should have_field('user_role_admin')
      page.should have_field('user_role_reader')
      page.should have_field('user_role_auditor')
      page.should have_field('user_role_searcher')
      page.should have_button('Save')
      page.should have_link('Cancel')
    end
    it "should allow a user's permissions to be changed" do
      uncheck('user_role_searcher')
      click_on('Save')
      click_link('Edit')
      page.should have_field('user_role_searcher', :checked => false)
    end
    it "should allow a user's username to be changed" do
      fill_in 'user_username', :with => "Test"
      click_on 'Save'
      click_link 'Edit'
      find_field('user_username').value.should == "Test"
    end
    it "should allow a user's name to be changed" do
      fill_in 'user_name', :with =>'Test'
      click_on 'Save'
      click_link 'Edit'
      find_field('user_name').value.should == 'Test'
    end
    it "should allow a user's email to be changed" do
      fill_in 'user_email', :with => "test@test.com"
      click_on 'Save'
      click_link 'Edit'
      find_field('user_email').value.should == "test@test.com"
    end
    it "should allow a user's password to be changed" do
      fill_in 'user_password', :with => 'newpass'
      fill_in  'user_password_confirmation', :with => 'newpass'
      click_on 'Save'
      click_on 'Log Out'
      fill_in 'user_session_username', :with => @user.username
      fill_in 'user_session_password', :with => 'newpass'
      click_on 'Log In'
      current_path.should == root_path
    end
    it "should redirect to the user's page when cancel is clicked" do
      click_link 'Cancel'
      current_path.should == users_path
    end
  end
  context "A user is being created" do
    before(:each) do
      visit users_path
      click_on 'New User'
    end
    it "should render the new user form" do
      current_path.should == new_user_path
      page.should have_field('user_name')
      page.should have_field('user_email')
      page.should have_field('user_password')
      page.should have_field('user_password_confirmation')

      page.should have_field('user_role_editor')
      page.should have_field('user_role_admin')
      page.should have_field('user_role_reader')
      page.should have_field('user_role_auditor')
      page.should have_field('user_role_searcher')
      page.should have_button('Save')
      page.should have_link('Cancel')
    end
    it "should allow the creation of a new user" do
      current_path.should == new_user_path
      create_a_user

      current_path.should == users_path
      page.all('table#users_table tr').count.should==3
      page.find('table', :id => 'users_table').text.should have_content('test')
      page.find('table', :id => 'users_table').text.should have_content('test name')
      page.find('table', :id => 'users_table').text.should have_content('test@test.com')
      page.find('table', :id => 'users_table').text.should have_content('editor, reader, auditor, searcher')
    end
  end
  context "The delete user button is pressed" do
    before(:each) do
      visit users_path
      click_on 'New User'
      create_a_user
    end
    it "should delete a user", js: true do
      click_on 'Delete'
      #Capybara does not support confirmation boxes through rails so just have to assume that the user clicks yes
      page.evaluate_script('window.confirm = function() { return true; }')
      page.should have_content('User Removed')
    end
  end

  def create_a_user
    fill_in 'user_username', :with => 'test'
    fill_in 'user_name', :with => 'test name'
    fill_in 'user_email', :with => 'test@test.com'
    fill_in 'user_password', :with => 'testpass'
    fill_in 'user_password_confirmation', :with => 'testpass'
    check 'user_role_editor'
    check 'user_role_reader'
    check 'user_role_auditor'
    check 'user_role_searcher'
    click_on 'Save'
  end
end