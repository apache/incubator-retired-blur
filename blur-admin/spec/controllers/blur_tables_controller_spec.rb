require "spec_helper"

describe BlurTablesController do
  describe "actions" do
    before(:each) do
      @zookeeper  = FactoryGirl.create :zookeeper
      @client = mock(Blur::Blur::Client)
      @blur_table = FactoryGirl.create :blur_table
      @cluster = FactoryGirl.create_list :cluster, 3
      @ability = Ability.new User.new

      @ability.stub!(:can?).and_return(true)
      @zookeeper.stub_chain(:blur_tables, :order).and_return [@blur_table]
      @zookeeper.stub_chain(:clusters, :order).and_return @cluster
      controller.stub!(:thrift_client).and_return(@client)
      controller.stub!(:current_ability).and_return(@ability)
      Zookeeper.stub!(:find_by_id).and_return(@zookeeper)
      Zookeeper.stub!(:first).and_return(@zookeeper)
    end

    describe "GET index" do
      before(:each) do
        Zookeeper.stub!(:order).and_return [@zookeeper]
      end

      it "should assign @zookeepers to be the collection of all zookeepers" do
        get :index
        assigns(:zookeepers).should == [@zookeeper]
      end

      it "should assign @current_zookeeper" do
        get :index
        assigns(:current_zookeeper).should == @zookeeper
      end

      it "should render the index template" do
        #need to test the new model setup
        get :index
        response.should render_template "index"
      end
      
      it "should assign @blur_tables to be the current zookeeper's blur_tables" do
        @zookeeper.should_receive(:blur_tables)
        get :index
        assigns(:blur_tables).should == [@blur_table]
      end

      it "should assign @clusters to be the current zookeeper's clusters" do
        @zookeeper.should_receive(:clusters)
        get :index
        assigns(:clusters).should == @cluster
      end
    end

    describe "GET Reload" do
      it "render_table_json should render JSON" do
        get :reload
        response.content_type.should == 'application/json'
        json = ActiveSupport::JSON.decode(response.body)
        json['clusters'].first['id'].should == @cluster.first.id
        json['tables'].first['id'].should == @blur_table.id
      end
    end

    describe "PUT enable" do
      before(:each) do
        @tables = [1, 2, 3]
        BlurTable.stub(:find).and_return @blur_table
        BlurTable.stub!(:enable)
        @blur_table.stub!(:save)
      end

      it "should enable all the given tables" do
        @tables.each do |id|
          BlurTable.should_receive(:find).with(id.to_s)
        end
        @blur_table.should_receive(:enable).exactly(@tables.length).times
        put :enable, :tables => @tables
      end

      #it "should render JSON" do
      #  put :enable, :tables => @tables
      #  response.content_type.should == 'application/json'
      #end
    end

    describe "PUT disable" do
      before(:each) do
        @tables = [1, 2, 3]
        BlurTable.stub(:find).and_return @blur_table
        BlurTable.stub!(:disable)
        @blur_table.stub!(:save)
      end

      it "should disable all the given tables" do
        @tables.each do |id|
          BlurTable.should_receive(:find).with(id.to_s)
        end
        @blur_table.should_receive(:disable).exactly(@tables.length).times
        put :disable, :tables => @tables
      end

      #it "should render JSON" do
      #  put :disable, :tables => @tables
      #  response.content_type.should == 'application/json'
      #end
    end

    describe "DELETE destroy" do
      before(:each) do
        @tables = [1, 2, 3]
        BlurTable.stub(:find).and_return @blur_table
        BlurTable.stub!(:blur_destroy)
        @blur_table.stub!(:save)
      end

      it "should destroy all the given tables" do
        @tables.each do |id|
          BlurTable.should_receive(:find).with(id.to_s)
        end
        @blur_table.should_receive(:blur_destroy).exactly(@tables.length).times
        delete :destroy, :tables => @tables
      end

      it "should set destroy index to true when the param is true" do
        @blur_table.should_receive(:blur_destroy).at_least(:once).with(true, kind_of(String))
        delete :destroy, :tables => @tables, :delete_index => 'true'
      end

      it "should set destroy index to false when the param is not true" do
        @blur_table.should_receive(:blur_destroy).at_least(:once).with(false, kind_of(String))
        delete :destroy, :tables => @tables, :delete_index => 'not true'
      end

      #it "should render JSON" do
      #  delete :destroy, :tables => @tables
      #  response.content_type.should == 'application/json'
      #end
    end

    describe "DELETE forget" do
      before(:each) do
        @tables = [1, 2, 3]
        BlurTable.stub(:destroy)
      end

      it "should forget all the given tables" do
        @tables.each do |id|
          BlurTable.should_receive(:destroy).with(id.to_s)
        end
        delete :forget, :tables => @tables
      end

      #it "should render JSON" do
      #  delete :forget, :tables => @tables
      #  response.content_type.should == 'application/json'
      #end
    end

    describe "GET schema" do
      before(:each) do
        BlurTable.stub(:find).and_return @blur_table
      end
      it "should the blur table whose schema is requested" do
        BlurTable.should_receive(:find).with @blur_table.id.to_s
        get :schema, :id => @blur_table.id
      end
      it "should render the schema partial" do
        get :schema, :id => @blur_table.id
        response.should render_template :partial => "_schema"
      end
    end

    describe "GET hosts" do
      before(:each) do
        BlurTable.stub(:find).and_return @blur_table
      end
      it "finds the blur table being whose hosts is requested" do
        BlurTable.should_receive(:find).with @blur_table.id.to_s
        get :hosts, :id => @blur_table.id
      end
      it "should render the hosts partial" do
        get :hosts, :id => @blur_table.id
        response.should render_template :partial => "_hosts"
      end
    end
  end
end
