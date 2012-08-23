require 'spec_helper'

describe Cluster do
  before(:each) do
    @cluster = FactoryGirl.create :cluster
    @options = {}
  end

  describe 'as_json' do
    it 'takes in no options' do
      @cluster.as_json(@options).should == {"id"=>@cluster.id, "name"=>@cluster.name, "safe_mode"=>false, "zookeeper_id"=>1, "can_update"=>false}
    end

    it 'takes in only options' do
      @options = {:only => [:id, :name]}
      @cluster.as_json(@options).should == {"id"=>@cluster.id, "name"=>@cluster.name, "can_update"=>false}
    end

    it 'takes in except options' do
      @options = {:except => [:safe_mode, :zookeeper_id]}
      @cluster.as_json(@options).should == {"id"=>@cluster.id, "name"=>@cluster.name, "can_update"=>false}
    end

    it 'sets can_update to false' do
      @cluster.as_json(@options).should == {"id"=>@cluster.id, "name"=>@cluster.name, "safe_mode"=>false, "zookeeper_id"=>1, "can_update"=>false}
    end

    it 'sets can_update to true' do
      @cluster.can_update = true
      @cluster.as_json(@options).should == {"id"=>@cluster.id, "name"=>@cluster.name, "safe_mode"=>false, "zookeeper_id"=>1, "can_update"=>true}
    end
  end
end
