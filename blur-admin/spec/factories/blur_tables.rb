FactoryGirl.define do
  factory :blur_table do
    sequence(:table_name) { |n| "Test Blur Table ##{n}" }
    current_size          { 10**12 + rand(999 * 10 ** 12) } #Between a terrabyte and a petabyte
    query_usage           { rand 500 }                      #Queries per second
    record_count          { 10**6 + rand(999 * 10 ** 6) }   #Between a million and a billion 
    status                { 1 + rand(2) }
    sequence(:table_uri)  { |n| "blur_table#{n}.blur.example.com" }
    table_analyzer        'standard.table_analyzer'
    table_schema          {{  :table => table_name, 
                              :setTable => true,
                              :setColumnFamilies  => true,  
                              :columnFamiliesSize => 3,
                              :columnFamilies     => { 'ColumnFamily1' => %w[Column1A Column1B Column1C],
                                                      'ColumnFamily2' => %w[Column2A Column2B Column2C],
                                                      'ColumnFamily3' => %w[Column3A Column3B Column3C] }}.to_json}
    server                {{  'Host1:101' => %w[shard-001 shard-002 shard-003],
                              'Host2:102' => %w[shard-004 shard-005 shard-006]}.to_json}
    ignore do
      recursive_factor 3
    end

    factory :blur_table_with_blur_query do
      after_create do |blur_table|
        FactoryGirl.create_list(:blur_query, 1, :blur_table => blur_table)
      end
    end

    factory :blur_table_with_blur_queries do
      after_create do |blur_table, evaluator|
        FactoryGirl.create_list(:blur_query, evaluator.recursive_factor, :blur_table => blur_table)
      end
    end
  end
end