class BlurController < ActiveRecord::Base
  belongs_to :zookeeper

  def as_json(options={})
    serial_properties = super(options)
    serial_properties.delete('updated_at')
    serial_properties.delete('created_at')
    serial_properties
  end
end
