FactoryGirl.define do
  factory :license do
    org             'NIC'
    expires_date    { Date.today.months_ago(-2) }
    issued_date     { Date.today.months_ago(6) }
    node_overage    { 0 }
    cluster_overage { 0 }
  end
end