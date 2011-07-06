require 'spec_helper'

describe ZookeepersController do
  before do
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    controller.stub!(:current_ability).and_return(@ability)

    # Set up association chain
    @zookeeper  = Factory.stub :zookeeper

    # ApplicationController.current_zookeeper
    Zookeeper.stub(:first).and_return @zookeeper
    session.delete(:current_zookeeper_id)
    # ApplicationController.zookeepers
    Zookeeper.stub(:all).and_return [@zookeeper]
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
    describe "with no previous zookeeper" do
      it "should set the current_zookeeper to be the first zookeeper found" do
        get :show_current, nil, nil
        assigns(:current_zookeeper).should == @zookeeper
      end
    end
    describe "with a valid pre-existing current_zookeeper" do
      it "should set the previous zookeeper to be the current_zookeeper" do
        old_zookeeper = Factory.stub :zookeeper
        Zookeeper.should_receive(:find_by_id).with(old_zookeeper.id).and_return(old_zookeeper)
        Zookeeper.should_receive(:find).with(old_zookeeper.id).and_return(old_zookeeper)
        get :show_current, nil, :current_zookeeper_id => old_zookeeper.id
        assigns(:current_zookeeper).should == old_zookeeper
      end
    end
    describe "with an invalid pre-existing current_zookeeper" do
      it "resets the session[:current_zookeeper_id] and redirects to the zookeeper path" do
        Zookeeper.should_receive(:find_by_id).with(1).and_return(nil)
        session.should_receive(:delete)
        get :show_current, nil, :current_zookeeper_id => 1

      end
    end
  end

  describe 'PUT make_current' do
    it "assigns the passed in id to the session" do
      put :make_current, :id => @zookeeper.id
      session[:current_zookeeper_id].should == @zookeeper.id
    end

    it 'renders the javascript redirect' do
      put :make_current
      response.body.should include "window.location ="
    end
  end
end
