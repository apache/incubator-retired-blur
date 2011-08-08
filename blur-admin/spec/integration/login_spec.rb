require 'spec_helper'

describe "the login process", :type => :request do
  context 'as an existing user on the sign in page' do

    # generate a valid user
    let(:user) { Factory.create :user }
    before do
      visit login_path
    end

    context 'with valid credentials' do
      before do
        fill_in 'Username', :with => user.username
        fill_in 'Password', :with => user.password
        click_button 'Log In'
      end

      it "should render the dashboard" do
        page.should have_selector '#zookeepers_wrapper'
      end
    end

    context 'with invalid password' do
      before do
        fill_in 'Username', :with => user.username
        fill_in 'Password', :with => 'invalid'
        click_button 'Log In'
      end

      it "should render the dashboard" do
        page.should have_selector '#zookeepers_wrapper'
      end
    end

    context 'with invalid password' do
      # generate a valid user
      let(:user) { Factory.create :user }

    end
  end
end
