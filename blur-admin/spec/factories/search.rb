FactoryGirl.define do
  factory :search do
    super_query     { rand(1) == 0 } # 50% chance
    column_object   { ["family_-sep-_ColumnFamily1", 
                       "column_-sep-_ColumnFamily2_-sep-_Column2A",
                       "column_-sep-_ColumnFamily2_-sep-_Column2B",
                       "column_-sep-_ColumnFamily3_-sep-_Column3C"] }
    fetch           { rand 10 ** 6 }
    offset          { rand 1 ** 5 }
    sequence(:name) {|n| "Search #{n}"}
    query           "employee.name:bob"
    blur_table_id   { rand 10 ** 6 }
    user_id         { rand 10 ** 6 }
  end
end