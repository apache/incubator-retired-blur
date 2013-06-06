class ChangeControllerstoBlurControllers < ActiveRecord::Migration
  def change
    rename_table :controllers, :blur_controllers
  end
end
