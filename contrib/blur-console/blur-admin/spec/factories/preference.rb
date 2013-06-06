FactoryGirl.define do
  factory :preference do
    name      'column'
    pref_type 'column'
    value     ['ColumnFamily']
  end

  factory :zookeeper_pref, class:Preference do
    name      '1'
    pref_type 'zookeeper'
    value     '1'
  end
end