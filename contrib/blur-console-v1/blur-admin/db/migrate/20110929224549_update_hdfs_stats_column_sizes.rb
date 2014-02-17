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

class UpdateHdfsStatsColumnSizes < ActiveRecord::Migration
  def self.up
    change_column :hdfs_stats, :config_capacity, :integer, :limit => 7
    change_column :hdfs_stats, :present_capacity, :integer, :limit => 7
    change_column :hdfs_stats, :dfs_remaining, :integer, :limit => 7
    change_column :hdfs_stats, :dfs_used, :integer, :limit => 7
    change_column :hdfs_stats, :under_replicated, :integer, :limit => 7
    change_column :hdfs_stats, :corrupt_blocks, :integer, :limit => 7
    change_column :hdfs_stats, :missing_blocks, :integer, :limit => 7
    change_column :hdfs_stats, :port, :string
    change_column :hdfs_stats, :dfs_used_percent, :float
  end

  def self.down
    change_column :hdfs_stats, :config_capacity, :integer
    change_column :hdfs_stats, :present_capacity, :integer
    change_column :hdfs_stats, :dfs_remaining, :integer
    change_column :hdfs_stats, :dfs_used, :integer
    change_column :hdfs_stats, :under_replicated, :integer
    change_column :hdfs_stats, :corrupt_blocks, :integer
    change_column :hdfs_stats, :missing_blocks, :integer
    change_column :hdfs_stats, :port, :integer
    change_column :hdfs_stats, :dfs_used_percent, :decimal
  end
end
