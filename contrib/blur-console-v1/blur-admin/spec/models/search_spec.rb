# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

require 'spec_helper'

describe Search do
  before(:each) do
    @user = FactoryGirl.create :user
    @search = FactoryGirl.create :search, :user_id => @user
  end

  describe 'blur_query' do
    it 'should create a BlurQuery object with its attributes' do
      User.stub!(:find).and_return(@user)
      Blur::SimpleQuery.should_receive(:new)
      Blur::BlurQuery.should_receive(:new)
      @search.blur_query
    end
  end

  describe 'column_object' do
    it 'should assign the column_object instance variable to the json parsed values when there are columns' do
      @search.column_object
      @search.instance_variable_get(:@column_object).should == ["family_-sep-_ColumnFamily1", 
                                                               "column_-sep-_ColumnFamily2_-sep-_Column2A",
                                                               "column_-sep-_ColumnFamily2_-sep-_Column2B",
                                                               "column_-sep-_ColumnFamily3_-sep-_Column3C"]
    end

    it 'should assign the column_object instance variable to and empty array when there arent any columns' do
      @search_without_columns = FactoryGirl.create :search, :user_id => @user, :columns => [].to_json
      @search_without_columns.column_object
      @search_without_columns.instance_variable_get(:@column_object).should == []
    end
  end

  describe 'column_families' do
    it 'should return an array of all the column families' do
      @search.column_families.should == ['ColumnFamily1']
    end
  end

  describe 'columns_hash' do
    it 'should create a hash from column family name (excluding full column families) to column' do 
      @search.columns_hash.should == {'ColumnFamily2' => ['recordId', 'Column2A', 'Column2B'], 'ColumnFamily3' => ['recordId', 'Column3C']}
    end
  end

  describe 'selector' do
    it 'should create a new selecrtor' do
      Blur::Selector.should_receive(:new)
      @search.selector
    end
  end

  describe 'fetch_results' do
    it 'should fetch the results from blur' do
      mock_client = mock(HdfsThriftClient::Client)
      mock_query = mock(Blur::BlurQuery)
      @search.stub!(:blur_query).and_return(mock_query)
      BlurThriftClient.should_receive(:client).with('URL').and_return(mock_client)
      mock_client.should_receive(:query).with('NAME', mock_query)
      @search.fetch_results('NAME', 'URL')
    end
  end

  describe 'schema' do
    it "should use the columns hash, column families, and the table schema to build a complete schema for the search" do
      @blur_table = FactoryGirl.create :blur_table
      result = @search.schema @blur_table
      result.each do |family|
        definition = family.last
        keys = definition.keys
        keys.should == ["name", "columns"]
        definition['columns'].first['name'].should == 'recordId'
      end
    end
  end
end
