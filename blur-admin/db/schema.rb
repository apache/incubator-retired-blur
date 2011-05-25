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

ActiveRecord::Schema.define(:version => 20110524213803) do

  create_table "blur_tables", :force => true do |t|
    t.string   "table_name"
    t.integer  "current_size"
    t.integer  "query_usage"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "hdfs_stats", :force => true do |t|
    t.string   "hdfs_name"
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
  end

end
