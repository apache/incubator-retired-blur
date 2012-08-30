# encoding: UTF-8
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

ActiveRecord::Schema.define(:version => 20120827172222) do

  create_table "audits", :force => true do |t|
    t.integer  "user_id"
    t.string   "mutation"
    t.string   "model_affected"
    t.string   "action"
    t.datetime "created_at",         :null => false
    t.datetime "updated_at",         :null => false
    t.string   "zookeeper_affected"
  end

  create_table "blur_controllers", :force => true do |t|
    t.integer "status"
    t.string  "blur_version"
    t.string  "node_name"
    t.integer "zookeeper_id"
  end

  add_index "blur_controllers", ["zookeeper_id"], :name => "index_controllers_on_zookeeper_id"

  create_table "blur_queries", :force => true do |t|
    t.text     "query_string",             :limit => 16777215
    t.integer  "complete_shards"
    t.integer  "uuid",                     :limit => 8
    t.datetime "created_at",                                   :null => false
    t.datetime "updated_at",                                   :null => false
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
    t.string   "times"
    t.integer  "total_shards"
    t.integer  "state"
    t.boolean  "record_only"
  end

  add_index "blur_queries", ["blur_table_id"], :name => "index_blur_queries_on_blur_table_id"

  create_table "blur_shards", :force => true do |t|
    t.integer "status"
    t.string  "blur_version"
    t.string  "node_name"
    t.integer "cluster_id"
  end

  add_index "blur_shards", ["cluster_id"], :name => "index_shards_on_cluster_id"

  create_table "blur_tables", :force => true do |t|
    t.string   "table_name"
    t.integer  "current_size",   :limit => 8
    t.integer  "query_usage"
    t.datetime "updated_at"
    t.integer  "record_count",   :limit => 8
    t.integer  "status"
    t.string   "table_uri"
    t.text     "table_analyzer"
    t.text     "table_schema"
    t.text     "server"
    t.integer  "cluster_id"
    t.integer  "row_count",      :limit => 8
    t.string   "comments"
  end

  add_index "blur_tables", ["cluster_id"], :name => "index_blur_tables_on_cluster_id"

  create_table "clusters", :force => true do |t|
    t.string  "name"
    t.integer "zookeeper_id"
    t.boolean "safe_mode"
  end

  add_index "clusters", ["zookeeper_id"], :name => "index_clusters_on_zookeeper_id"

  create_table "hdfs", :force => true do |t|
    t.string "host"
    t.string "port"
    t.string "name"
  end

  create_table "hdfs_stats", :force => true do |t|
    t.integer  "config_capacity",  :limit => 8
    t.integer  "present_capacity", :limit => 8
    t.integer  "dfs_remaining",    :limit => 8
    t.integer  "dfs_used_real",    :limit => 8
    t.float    "dfs_used_percent"
    t.integer  "under_replicated", :limit => 8
    t.integer  "corrupt_blocks",   :limit => 8
    t.integer  "missing_blocks",   :limit => 8
    t.integer  "total_nodes"
    t.integer  "dead_nodes"
    t.datetime "created_at",                    :null => false
    t.string   "host"
    t.string   "port"
    t.integer  "hdfs_id"
    t.integer  "live_nodes"
    t.integer  "dfs_used_logical", :limit => 8
  end

  add_index "hdfs_stats", ["hdfs_id"], :name => "index_hdfs_stats_on_hdfs_id"

  create_table "licenses", :id => false, :force => true do |t|
    t.string  "org"
    t.date    "issued_date"
    t.date    "expires_date"
    t.integer "node_overage"
    t.integer "grace_period_days_remain"
    t.integer "cluster_overage"
  end

  create_table "preferences", :force => true do |t|
    t.string   "name"
    t.string   "pref_type"
    t.text     "value"
    t.datetime "created_at", :null => false
    t.datetime "updated_at", :null => false
    t.integer  "user_id"
  end

  add_index "preferences", ["user_id"], :name => "index_preferences_on_user_id"

  create_table "searches", :force => true do |t|
    t.boolean  "super_query"
    t.text     "columns"
    t.integer  "fetch"
    t.integer  "offset"
    t.string   "name"
    t.string   "query"
    t.integer  "blur_table_id"
    t.integer  "user_id"
    t.datetime "created_at",                       :null => false
    t.datetime "updated_at",                       :null => false
    t.boolean  "record_only",   :default => false
  end

  add_index "searches", ["blur_table_id"], :name => "index_searches_on_blur_table_id"
  add_index "searches", ["user_id"], :name => "index_searches_on_user_id"

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

  add_index "system_metrics", ["parent_id"], :name => "index_system_metrics_on_parent_id"
  add_index "system_metrics", ["request_id"], :name => "index_system_metrics_on_request_id"
  add_index "system_metrics", ["transaction_id"], :name => "index_system_metrics_on_transaction_id"

  create_table "users", :force => true do |t|
    t.string   "username"
    t.string   "email"
    t.string   "crypted_password"
    t.string   "password_salt"
    t.string   "persistence_token"
    t.datetime "created_at",        :null => false
    t.datetime "updated_at",        :null => false
    t.integer  "roles_mask"
    t.string   "name"
  end

  create_table "zookeepers", :force => true do |t|
    t.string  "url"
    t.string  "name"
    t.integer "status"
    t.string  "blur_urls", :limit => 4000
  end

end
