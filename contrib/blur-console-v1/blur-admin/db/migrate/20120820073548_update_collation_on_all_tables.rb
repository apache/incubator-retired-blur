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

class UpdateCollationOnAllTables < ActiveRecord::Migration
  def up
    execute "ALTER TABLE audits DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE audits CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE blur_queries DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE blur_queries CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE blur_tables DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE blur_tables CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE clusters DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE clusters CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE controllers DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE controllers CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE hdfs DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE hdfs CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE hdfs_stats DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE hdfs_stats CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE licenses DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE licenses CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE preferences DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE preferences CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE searches DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE searches CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE shards DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE shards CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE users DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE users CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
    
    execute "ALTER TABLE zookeepers DEFAULT CHARACTER SET utf8 COLLATE utf8_bin"
    execute "ALTER TABLE zookeepers CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin"
  end
end
