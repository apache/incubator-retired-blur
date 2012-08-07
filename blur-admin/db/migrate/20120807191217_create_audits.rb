class CreateAudits < ActiveRecord::Migration
  def change
    create_table :audits do |t|
      t.string :user
      t.integer :mutation
      t.integer :model_affected
      t.string :action

      t.timestamps
    end
  end
end
