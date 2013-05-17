require 'spec_helper'
describe ApplicationHelper do  
  describe "pluralize without count" do
    it "returns the singular if given a 1 or '1'" do 
      pluralize_no_count(1, 'rail').should == 'rail'
      pluralize_no_count('1', 'rail').should == 'rail'
    end

    it 'should return the system defined plural if given a numbe larger than 1' do
      pluralize_no_count(2, 'rail').should == 'rails'
    end

    it 'should return the given plural if given a numbe larger than 1' do
      pluralize_no_count(2, 'rail', 'not rail').should == 'not rail'
    end
  end

  describe "stateful nav url" do
    it "should return an empty string if there isnt a current zookeeper" do
      session[:current_zookeeper_id] = nil
      stateful_nav_url('page').should == ""
    end

    it "should return an empty string if given page isnt stateful" do
      session[:current_zookeeper_id] = 1
      stateful_nav_url('page').should == ""
    end

    it "given a valid stateful page should return the correct page" do
      session[:current_zookeeper_id] = 1
      stateful_nav_url('environment').should == zookeeper_path(1)
      stateful_nav_url('blur_table').should == zookeeper_blur_tables_path(1)
      stateful_nav_url('blur_query').should == zookeeper_blur_queries_path(1)
      stateful_nav_url('search').should == zookeeper_searches_path(1)
    end
  end
end
