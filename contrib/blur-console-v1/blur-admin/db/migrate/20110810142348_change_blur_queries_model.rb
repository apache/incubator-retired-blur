#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#  
#  http://www.apache.org/licenses/LICENSE-2.0
#  
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

class ChangeBlurQueriesModel < ActiveRecord::Migration
  def self.up
    add_column :blur_queries, :times, :string
    remove_column :blur_queries, :cpu_time
    remove_column :blur_queries, :real_time

    remove_column :blur_queries, :interrupted
    remove_column :blur_queries, :running

    rename_column :blur_queries, :complete, :complete_shards
    add_column :blur_queries, :total_shards, :integer

    add_column :blur_queries, :state, :integer

  end

  def self.down
    remove_column :blur_queries, :times
    add_column :blur_queries, :cpu_time, :integer
    add_column :blur_queries, :real_time, :integer

    add_column :blur_queries, :interrupted, :boolean
    add_column :blur_queries, :running, :boolean

    rename_column :blur_queries, :complete_shards, :complete
    remove_column :blur_queries, :total_shards

    remove_column :blur_queries, :state
  end
end
