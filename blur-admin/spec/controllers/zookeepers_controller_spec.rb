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
    describe 'destroy zookeeper' do
      before do
        @zookeeper.stub!(:destroy)
      end

      it "destroys the zookeeper" do
        @zookeeper.should_receive(:destroy)
        @zookeeper.stub!(:status).and_return 0
        delete :destroy, :id => @zookeeper.id, :format => :json
      end

      it "errors when the zookeeper is enabled" do
        expect {
          @zookeeper.stub!(:status).and_return 1
          delete :destroy, :id => @zookeeper.id, :format => :json
        }.to raise_error
      end

      it "logs the event when the zookeeper is deleted" do
        @zookeeper.stub!(:status).and_return 0
        @zookeeper.stub!(:destroyed?).and_return true
        Audit.should_receive :log_event
        delete :destroy, :id => @zookeeper.id, :format => :json
      end
    end
  end
end
