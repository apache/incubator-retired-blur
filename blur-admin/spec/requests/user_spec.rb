require 'spec_helper'

describe "Account Page" do
  before(:each) do
    setup_tests
    visit user_path(@user.id)
  end

  context"The page is loaded" do
    it "should load the correct view elements" do
      page.should have_css('div', :id => 'pref_col')
      page.should have_css('div', :id => 'my-cols')
      page.should have_css('div', :id => 'actual-trash')
      page.should have_css('div', :id => 'pref-key')
      page.should have_css('div', :id => 'opt-col')
      page.should have_css('div', :id => 'zookeeper-pref')
      page.should have_select('zookeeper_pref')
      page.should have_select('zookeeper_num')
      page.should have_button('zookeeper_submit')
      page.should have_css('div', :id => 'user-info')
      page.should have_link('Edit')
      page.should have_link('View All Users')
    end
  end
  context "The Edit link is clicked" do
    it "should redirect to the edit user page for that user's id" do
      click_on 'Edit'
      current_path.should == edit_user_path(@user.id)
    end
  end
  context "The View All Users link is clicked" do
    it "should redirect to the admin page" do
      click_on 'View All Users'
      current_path.should == users_path
    end
  end
  context "Column family preferences" do
    it "should display a column family under saved column families after it is clicked" do
      find('#opt-col').find('#value_ColumnFamily1').click
      find('#my-cols').should have_css('div', :id => 'value_ColumnFamily1')
    end
    it "should remove a column family from the set of saved column families after it is clicked again" do
      find('#opt-col').find('#value_ColumnFamily1').click
      find('#my-cols').should have_css('div', :id => 'no-saved', :style =>'display: none')
      find('#opt-col').find('#value_ColumnFamily1').click
      find('#my-cols').should have_css('div', :id => 'no-saved', :style =>'display: block')
    end
  end
  context "Zookeeper preferences" do
    it "should default to the Default zookeeper preference" do
      find_field('zookeeper_pref').value.should =='0'
      page.should have_css('select', :id => 'zookeeper_num', :style => 'display: none')
      page.should have_css('input', :id => 'zookeeper_submit', :disabled => 'disabled') 
    end
    it "should display Zookepeer selector when preference is changed to 'Choose Zookeeper'" do
      select 'Choose Zookeeper', :from => 'zookeeper_pref'
      page.should have_css('select', :id => 'zookeeper_num', :style =>'display: inline-block')
    end
    it "should hide the Zookeeper selector when either Default or Use Last Zookeeper is selected" do
      select 'Choose Zookeeper', :from => 'zookeeper_pref'
      page.should have_css('select', :id => 'zookeeper_num', :style =>'display: inline-block')
      select 'Use Last Zookeeper', :from => 'zookeeper_pref'
      page.should have_css('select', :id => 'zookeeper_num', :style => 'display:none')
      select 'Choose Zookeeper', :from => 'zookeeper_pref'
      page.should have_css('select', :id => 'zookeeper_num', :style =>'display: inline-block')
      select 'Default', :from => 'zookeeper_pref'
      page.should have_css('select', :id => 'zookeeper_num', :style => 'display: none')
    end
  end
end