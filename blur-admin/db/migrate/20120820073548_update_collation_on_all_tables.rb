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
