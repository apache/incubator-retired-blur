class AddOnlineEnsembleNodes < ActiveRecord::Migration
  def change
    add_column :zookeepers, :online_ensemble_nodes, :string
  end
end
