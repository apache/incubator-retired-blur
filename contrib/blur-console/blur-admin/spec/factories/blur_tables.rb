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

FactoryGirl.define do
  factory :blur_table do
    sequence(:table_name) { |n| "Test Blur Table ##{n}" }
    current_size          { 10**12 + rand(999 * 10 ** 12) } #Between a terrabyte and a petabyte
    query_usage           { rand 500 }                      #Queries per second
    query_count           { rand 10 }
    record_count          { 10**6 + rand(999 * 10 ** 6) }   #Between a million and a billion 
    row_count             { 10**6 + rand(999 * 10 ** 6) }   #Between a million and a billion 
    table_status                { 1 + rand(2) }
    sequence(:table_uri)  { |n| "blur_table#{n}.blur.example.com" }
    table_analyzer        'standard.table_analyzer'
    comments              'comment'
    cluster               { FactoryGirl.create(:cluster) }
    table_schema          {[
                            {
                              "name" => 'ColumnFamily1',
                              "columns" => [
                                {"name" => 'Column1A'},
                                {"name" => 'Column1B'},
                                {"name" => 'Column1C'}
                              ]
                            },
                            {
                              "name" => 'ColumnFamily2',
                              "columns" => [
                                {"name" => 'Column2A'},
                                {"name" => 'Column2B'},
                                {"name" => 'Column2C'}
                              ]
                            },
                            {
                              "name" => 'ColumnFamily3',
                              "columns" => [
                                {"name" => 'Column3A'},
                                {"name" => 'Column3B'},
                                {"name" => 'Column3C'}
                              ]
                            }
                          ].to_json}
    server                {{  'Host1:101' => %w[shard-001 shard-002 shard-003],
                              'Host2:102' => %w[shard-004 shard-005 shard-006]}.to_json}
    ignore do
      recursive_factor 3
    end

    factory :blur_table_with_blur_query do
      after_create do |blur_table|
        FactoryGirl.create_list(:blur_query, 1, :blur_table => blur_table)
      end
    end

    factory :blur_table_with_blur_queries do
      after_create do |blur_table, evaluator|
        FactoryGirl.create_list(:blur_query, evaluator.recursive_factor, :blur_table => blur_table)
      end
    end
  end
end
