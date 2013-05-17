FactoryGirl.define do
  factory :blur_query do
    sequence(:query_string) {|n| "Blur Query ##{n} Query String"}
    complete_shards         { rand 6 }
    uuid                    { rand 10 ** 8 }
    super_query_on          { true } # 75% chance
    start                   { rand 10 ** 6 }
    fetch_num               { rand 10 ** 6 }
    userid                  { "Test User ##{rand 20}" }
    times                   {{'shard' => { :cpuTime => 40, :realTime => 56, :setCpuTime => true, :setRealTime => true }}.to_json}
    total_shards            { 5 }
    state                   { rand 3 }
  end
end