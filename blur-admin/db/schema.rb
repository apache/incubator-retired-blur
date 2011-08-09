# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended to check this file into your version control system.

ActiveRecord::Schema.define(:version => 20110809202748) do

  create_table "blur_queries", :force => true do |t|
    t.string   "query_string"
    t.integer  "cpu_time"
    t.integer  "real_time"
    t.integer  "complete"
    t.boolean  "interrupted"
    t.boolean  "running"
    t.integer  "uuid",                     :limit => 8
    t.datetime "created_at"
    t.datetime "updated_at"
    t.boolean  "super_query_on"
    t.string   "facets"
    t.integer  "start"
    t.integer  "fetch_num"
    t.text     "pre_filters",              :limit => 16777215
    t.text     "post_filters",             :limit => 16777215
    t.text     "selector_column_families"
    t.text     "selector_columns"
    t.string   "userid"
    t.integer  "blur_table_id"
  end

  create_table "blur_tables", :force => true do |t|
    t.string   "table_name"
    t.integer  "current_size",   :limit => 8
    t.integer  "query_usage"
    t.datetime "created_at"
    t.datetime "updated_at"
    t.integer  "record_count",   :limit => 8
    t.integer  "status"
    t.string   "table_uri"
    t.text     "table_analyzer"
    t.text     "table_schema"
    t.text     "server"
    t.integer  "cluster_id"
  end

  create_table "clusters", :force => true do |t|
    t.string   "name"
    t.integer  "zookeeper_id"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "controllers", :force => true do |t|
    t.integer  "status"
    t.string   "blur_version"
    t.string   "node_name"
    t.string   "node_location"
    t.integer  "zookeeper_id"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "hdfs", :force => true do |t|
    t.string   "host"
    t.string   "port"
    t.string   "name"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "hdfs_stats", :force => true do |t|
    t.integer  "config_capacity"
    t.integer  "present_capacity"
    t.integer  "dfs_remaining"
    t.integer  "dfs_used"
    t.decimal  "dfs_used_percent", :precision => 10, :scale => 0
    t.integer  "under_replicated"
    t.integer  "corrupt_blocks"
    t.integer  "missing_blocks"
    t.integer  "total_nodes"
    t.integer  "dead_nodes"
    t.datetime "created_at"
    t.datetime "updated_at"
    t.string   "host"
    t.integer  "port"
    t.integer  "hdfs_id"
  end

  create_table "preferences", :force => true do |t|
    t.string   "name"
    t.string   "pref_type"
    t.text     "value"
    t.datetime "created_at"
    t.datetime "updated_at"
    t.integer  "user_id"
  end

  create_table "searches", :force => true do |t|
    t.boolean  "super_query"
    t.text     "columns"
    t.integer  "fetch"
    t.integer  "offset"
    t.string   "name"
    t.string   "query"
    t.integer  "blur_table_id"
    t.integer  "user_id"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "shards", :force => true do |t|
    t.integer  "status"
    t.string   "blur_version"
    t.string   "node_name"
    t.string   "node_location"
    t.integer  "cluster_id"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "system_metrics", :force => true do |t|
    t.string   "name",               :null => false
    t.datetime "started_at",         :null => false
    t.string   "transaction_id"
    t.text     "payload"
    t.float    "duration",           :null => false
    t.float    "exclusive_duration", :null => false
    t.integer  "request_id"
    t.integer  "parent_id"
    t.string   "action",             :null => false
    t.string   "category",           :null => false
  end

  create_table "users", :force => true do |t|
    t.string   "username"
    t.string   "email"
    t.string   "crypted_password"
    t.string   "password_salt"
    t.string   "persistence_token"
    t.datetime "created_at"
    t.datetime "updated_at"
    t.integer  "roles_mask"
  end

  create_table "zookeepers", :force => true do |t|
    t.string  "url"
    t.string  "name"
    t.integer "status"
    t.string  "host"
    t.string  "port"
  end

end
