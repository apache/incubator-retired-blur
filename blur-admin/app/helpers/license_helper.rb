module LicenseHelper 
  def license_text(license)
    if license
      days_left_in_license = (license.expires_date - Date.today).to_i
      text = "Licensed to: #{license.org} on #{license.issued_date.strftime('%d %b %Y')}."
      if days_left_in_license < 0
        text << " License is expired."
      elsif days_left_in_license == 0
        text << " License expires today."
      elsif days_left_in_license < 30
        text << " License expires in #{pluralize(days_left_in_license, 'day')}."
      end
      
      if license.node_overage > 0
        text << " There are currently #{pluralize(license.node_overage, 'node')} over licensed amount."
        if license.grace_period_days_remain < 0
          text << " Please contact Near Infinity to upgrade the license."
        else
          text << " #{pluralize(license.grace_period_days_remain, 'day')} left until new license is needed."
        end
      end
      text
    else
      "No valid license found."
    end
  end
  
  def footer_class(license)
    (license.nil? || (license.expires_date - Date.today).to_i < 30 || license.grace_period_days_remain < 10) && 'expiring_license'
  end
end