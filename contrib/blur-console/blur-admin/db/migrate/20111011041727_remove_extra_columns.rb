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

class RemoveExtraColumns < ActiveRecord::Migration
  def self.up
    remove_column :shards, :created_at
    remove_column :shards, :updated_at
    remove_column :hdfs_stats, :updated_at
    remove_column :hdfs, :created_at
    remove_column :hdfs, :updated_at
    remove_column :controllers, :created_at
    remove_column :controllers, :updated_at
    remove_column :clusters, :created_at
    remove_column :clusters, :updated_at
    remove_column :blur_tables, :created_at
  end

  def self.down
    add_column :shards, :created_at, :datetime
    add_column :shards, :updated_at, :datetime
    add_column :hdfs_stats, :updated_at, :datetime
    add_column :hdfs, :created_at, :datetime
    add_column :hdfs, :updated_at, :datetime
    add_column :controllers, :created_at, :datetime
    add_column :controllers, :updated_at, :datetime
    add_column :clusters, :created_at, :datetime
    add_column :clusters, :updated_at, :datetime
    add_column :blur_tables, :created_at, :datetime
  end
end
