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

class UpdateColumnsOnBlurZookeeperInstances < ActiveRecord::Migration
  def self.up
    add_column :blur_zookeeper_instances, :name, :string

    remove_column :blur_zookeeper_instances, :port
    remove_column :blur_zookeeper_instances, :created_at
    remove_column :blur_zookeeper_instances, :updated_at

    rename_column :blur_zookeeper_instances, :host, :url
  end

  def self.down
    remove_column :blur_zookeeper_instances, :name

    add_column :blur_zookeeper_instances, :port, :string
    add_column :blur_zookeeper_instances, :created_at, :datetime
    add_column :blur_zookeeper_instances, :updated_at, :datetime

    rename_column :blur_zookeeper_instances, :url, :host
  end
end