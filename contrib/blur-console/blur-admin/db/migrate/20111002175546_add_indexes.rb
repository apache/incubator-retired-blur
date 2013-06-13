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

class AddIndexes < ActiveRecord::Migration
  def self.up
    add_index :blur_queries, :blur_table_id
    add_index :blur_tables, :cluster_id
    add_index :clusters, :zookeeper_id
    add_index :controllers, :zookeeper_id
    add_index :hdfs_stats, :hdfs_id
    add_index :preferences, :user_id
    add_index :searches, :blur_table_id
    add_index :searches, :user_id
    add_index :shards, :cluster_id
    add_index :system_metrics, :transaction_id
    add_index :system_metrics, :request_id
    add_index :system_metrics, :parent_id
  end

  def self.down
    remove_index :blur_queries, :blur_table_id
    remove_index :blur_tables, :cluster_id
    remove_index :clusters, :zookeeper_id
    remove_index :controllers, :zookeeper_id
    remove_index :hdfs_stats, :hdfs_id
    remove_index :preferences, :user_id
    remove_index :searches, :blur_table_id
    remove_index :searches, :user_id
    remove_index :shards, :cluster_id
    remove_index :system_metrics, :transaction_id
    remove_index :system_metrics, :request_id
    remove_index :system_metrics, :parent_id
  end
end
