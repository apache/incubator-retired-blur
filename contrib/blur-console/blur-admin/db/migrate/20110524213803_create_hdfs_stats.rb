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

class CreateHdfsStats < ActiveRecord::Migration
  def self.up
    create_table :hdfs_stats do |t|
      t.string :hdfs_name
      t.integer :config_capacity
      t.integer :present_capacity
      t.integer :dfs_remaining
      t.integer :dfs_used
      t.decimal :dfs_used_percent
      t.integer :under_replicated
      t.integer :corrupt_blocks
      t.integer :missing_blocks
      t.integer :total_nodes
      t.integer :dead_nodes

      t.timestamps
    end
  end

  def self.down
    drop_table :hdfs_stats
  end
end
