require 'spec_helper'

describe ZookeepersController do
  describe "actions" do
    before do
      @ability = Ability.new User.new
      @ability.stub!(:can?).and_return(true)
      controller.stub!(:current_ability).and_return(@ability)

      # Set up association chain
      @zookeeper  = FactoryGirl.create :zookeeper

      # ApplicationController.current_zookeeper
      Zookeeper.stub(:find_by_id).and_return(@zookeeper)
      Zookeeper.stub!(:first).and_return(@zookeeper)
      # ApplicationController.zookeepers
      Zookeeper.stub(:order).and_return [@zookeeper]
    end

    describe 'GET index' do
      it "assigns the collection all zookeepers to @zookeepers" do
        Zookeeper.stub_chain(:select, :order).and_return [@zookeeper]
        get :index
        assigns(:zookeepers).should == [@zookeeper]
      end
      it "renders the index template" do
        get :index
        response.should render_template 'index'
      end
    end

    describe 'GET show' do
      it "assigns the current zookeeper to @zookeeper" do
        get :show, :id => @zookeeper.id
        assigns(:current_zookeeper).should == @zookeeper
      end

      it "assigns the current zookeeper to @zookeeper" do
        get :show, :id => @zookeeper.id
        assigns(:current_zookeeper).should == @zookeeper
        session[:current_zookeeper_id].should == @zookeeper.id
      end

      it "assigns the shards nodes and the controller nodes" do
        @zookeeper.stub_chain(:shards, :count).and_return(1)
        @zookeeper.stub_chain(:controllers, :count).and_return(1)
        get :show, :id => @zookeeper.id
        assigns(:shard_nodes).should == 1
        assigns(:controller_nodes).should == 1
      end

      it "renders the show_current view" do
        get :show, :id => @zookeeper.id
        response.should render_template :show
      end

      describe "testing ApplicationController current ZK logic" do
        before :each do
          Zookeeper.unstub!(:find_by_id)
        end

        it "with more than one zookeeper it should set the current_zookeeper to be the ZK with the session ID" do
          Zookeeper.should_receive(:find_by_id).twice.with('1').and_return @zookeeper
          get :show, :id => 1
          assigns(:current_zookeeper).should == @zookeeper
          session[:current_zookeeper_id].should == @zookeeper.id
        end

        it "should not set the session ID if no ZK is found and should redirect to the root path" do
          get :show, nil, nil
          assigns(:current_zookeeper).should == nil
          session[:current_zookeeper_id].should == nil
          response.should redirect_to :root
        end

        it "should not set the session ID if no ZK is found and should redirect to the root path for xhr requests" do
          Zookeeper.stub!(:first).and_return nil
          xhr :get, :show, :id => '23456'
          assigns(:current_zookeeper).should == nil
          session[:current_zookeeper_id].should == nil
          response.response_code.should == 409
        end
      end
    end

    describe 'GET dashboard' do
      it "collects the long queries data" do
        pending 'need to test that all the different aspects of the query are being returned'
        get :dashboard
      end

      it "renders a json object" do
        get :dashboard
        response.content_type.should == 'application/json'
      end
    end

    describe 'DELETE destroy_shard' do
      before :each do
        @shard = FactoryGirl.create :shard
      end

      it "calls destroy on the clusters model" do
        Zookeeper.stub_chain(:find, :shards, :find_by_id).and_return(@shard)
        @shard.should_receive(:destroy)
        delete :destroy_shard, :shard_id => @shard.id, :id => @zookeeper.id
      end

      it "doesnt call destroy on nil clusters model" do
        @shard.should_not_receive(:destroy)
        delete :destroy_shard, :shard_id => @shard.id, :id => @zookeeper.id
      end

      it "redirects to the zookeeper page" do
        delete :destroy_shard, :shard_id => @shard.id, :id => @zookeeper.id
        response.should redirect_to :zookeeper
      end
    end

    describe 'DELETE destroy_cluster' do
      before :each do
        @cluster = FactoryGirl.create :cluster
      end

      it "calls destroy on the clusters model" do
        Zookeeper.stub_chain(:find, :clusters, :find_by_id).and_return(@cluster)
        @cluster.should_receive(:destroy)
        delete :destroy_cluster, :cluster_id => @cluster.id, :id => @zookeeper.id
      end

      it "doesnt call destroy on nil clusters model" do
        @cluster.should_not_receive(:destroy)
        delete :destroy_cluster, :cluster_id => @cluster.id, :id => @zookeeper.id
      end

      it "redirects to the zookeeper page" do
        delete :destroy_cluster, :cluster_id => @cluster.id, :id => @zookeeper.id
        response.should redirect_to :zookeeper
      end
    end

    describe 'DELETE destroy_controller' do
      before(:each) do
        @created_controller = FactoryGirl.create :controller
      end

      it "calls destroy on the Controller model" do
        Zookeeper.stub_chain(:find, :controllers, :find_by_id).and_return(@created_controller)
        @created_controller.should_receive(:destroy)
        delete :destroy_controller, :controller_id => @created_controller.id, :id => @zookeeper
      end

      it "doesnt call destroy on nil controllers model" do
        @created_controller.should_not_receive(:destroy)
        delete :destroy_controller, :controller_id => @created_controller.id, :id => @zookeeper.id
      end

      it "redirects to the zookeeper page" do
        delete :destroy_controller, :controller_id => @created_controller.id, :id => @zookeeper
        response.should redirect_to :zookeeper
      end
    end

    describe 'DELETE destroy_zookeeper' do
      it "calls destroy on the Zookeeper model" do
        Zookeeper.stub!(:find).and_return(@zookeeper)
        @zookeeper.should_receive(:destroy)
        delete :destroy, :id => @zookeeper.id
      end

      it "doesnt call destroy on the nil Zookeeper model" do
        @zookeeper.should_not_receive(:destroy)
        delete :destroy, :id => @zookeeper.id
      end

      it "redirects to the zookeeper page" do
        delete :destroy, :id => @zookeeper.id
        response.should redirect_to :zookeeper
      end
    end
  end
end