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

class RenameBlurZookeeperInstanceId < ActiveRecord::Migration
  def self.up
    rename_column :clusters, :blur_zookeeper_instance_id, :zookeeper_id
    rename_column :controllers, :blur_zookeeper_instance_id, :zookeeper_id
  end

  def self.down
    rename_column :clusters, :zookeeper_id, :blur_zookeeper_instance_id
    rename_column :controllers, :zookeeper_id, :blur_zookeeper_instance_id
  end
end
