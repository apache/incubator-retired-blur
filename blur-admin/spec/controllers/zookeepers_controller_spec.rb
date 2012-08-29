require 'spec_helper'

describe ZookeepersController do
  describe "actions" do
    before do
      # Universal setup
      setup_tests

      # Set up association chain
      @zookeeper  = FactoryGirl.create :zookeeper
      Zookeeper.stub!(:find).and_return @zookeeper
    end

    describe 'GET index' do
      it "renders the index template" do
        get :index, :format => :html
        response.should render_template 'index'
      end

      it "should set the zookeeper when there is only one zookeeper" do
        Zookeeper.stub!(:count).and_return 1
        Zookeeper.stub!(:first).and_return @zookeeper
        controller.stub!(:set_zookeeper_with_preference)
        controller.should_receive(:set_zookeeper).with(@zookeeper.id)
        get :index, :format => :html
      end
    end

    describe 'GET show' do
      it "renders the show_current view" do
        get :show, :id => @zookeeper.id, :format => :html
        response.should render_template :show
      end

      it "renders json when the format is json" do
        get :show, :id => @zookeeper.id, :format => :json
        response.body.should == @zookeeper.to_json(:methods => [:clusters, :blur_controllers])
      end
    end

    describe 'GET long running queries' do
      before :each do
        @zookeeper.stub!(:long_running_queries)
      end

      it "it grabs the long_running_queries and renders a json object" do
        @zookeeper.should_receive :long_running_queries
        get :long_running_queries, :id => @zookeeper.id, :format => :json
        response.content_type.should == 'application/json'
      end
    end
  end
end
