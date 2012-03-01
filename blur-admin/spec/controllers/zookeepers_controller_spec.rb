require 'spec_helper'

describe ZookeepersController do
  describe "actions" do
    before do
      @ability = Ability.new User.new
      @ability.stub!(:can?).and_return(true)
      controller.stub!(:current_ability).and_return(@ability)

      # Set up association chain
      @zookeeper  = Factory.stub :zookeeper

      # ApplicationController.current_zookeeper
      Zookeeper.stub(:find_by_id).and_return(@zookeeper)
      # Zookeeper.stub_chain(:order, :first).and_return @zookeeper
      # ApplicationController.zookeepers
      Zookeeper.stub(:order).and_return [@zookeeper]
    end

    describe 'GET show_current' do
      it "assigns the collection all zookeepers to @zookeepers" do
        get :show_current
        assigns(:zookeepers).should == [@zookeeper]
      end

      it "assigns the current zookeeper to @current_zookeeper" do
        get :show_current
        assigns(:current_zookeeper).should == @zookeeper
      end

      it "assigns the current zookeeper to @zookeeper" do
        get :show_current
        assigns(:zookeeper).should == @zookeeper
      end

      it "renders the show_current view" do
        get :show_current
        response.should render_template :show_current
      end
      describe "with persisted zookeepers" do
        describe "with no previous zookeeper" do
          it "should set the current_zookeeper to be the first zookeeper found" do
            get :show_current, nil, nil
            assigns(:current_zookeeper).should == @zookeeper
            session[:current_zookeeper_id].should == @zookeeper.id
          end
        end
        describe "with a valid pre-existing current_zookeeper" do
          it "should set the previous zookeeper to be the current_zookeeper" do
            old_zookeeper = Factory.stub :zookeeper
            Zookeeper.should_receive(:find_by_id).with(old_zookeeper.id).and_return(old_zookeeper)
            get :show_current, nil, :current_zookeeper_id => old_zookeeper.id
            assigns(:current_zookeeper).should == old_zookeeper
            session[:current_zookeeper_id].should == old_zookeeper.id
          end
        end
        describe "with an invalid pre-existing current_zookeeper" do
          it "sets current_zookeeper to the first zookeeper, and resets the session" do
            Zookeeper.should_receive(:find_by_id).with(1).and_return(@zookeeper)
            get :show_current, nil, :current_zookeeper_id => 1
            assigns(:current_zookeeper).should == @zookeeper
            session[:current_zookeeper_id].should == @zookeeper.id
          end
        end
      end
      describe "with no persisted zookeepers" do
        describe "with no previous zookeeper" do
          it "redirects to the root path, with no current_zookeeper_id in session" do
            Zookeeper.should_receive(:find_by_id).with(1).and_return(nil)
            Zookeeper.stub_chain(:order, :first).and_return nil
            get :show_current, nil, :current_zookeeper_id => 1
            session[:current_zookeeper_id].should be nil
            response.should redirect_to root_path
          end
        end
        describe "with a previous current zookeeper" do
          it "redirects to the root path, with no current_zookeeper_id in session" do
            Zookeeper.should_receive(:find_by_id).with(1).and_return(nil)
            Zookeeper.stub_chain(:order, :first).and_return nil
            get :show_current, nil, :current_zookeeper_id => 1
            session[:current_zookeeper_id].should be nil
            response.should redirect_to root_path
          end
        end
      end
    end

    describe 'PUT make_current' do
      it "assigns the passed in id to the session" do
        put :make_current, :id => @zookeeper.id
        session[:current_zookeeper_id].should == @zookeeper.id.inspect
      end

      it 'renders the javascript redirect' do
        put :make_current
        response.body.should include "window.location ="
      end
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

    describe 'GET dashboard' do
      it "renders a json object"
      it "collects the long queries data" do
        get :dashboard
      end
    end
  end
end