class Pounder
  def self.run
    # Setup object heirarchy
    zk = Zookeeper.create!(:name => "Pounder-zk")
    cluster = Cluster.create!(:name => "Pounder-cluster", :zookeeper => zk)
    table = BlurTable.create!(:table_name => "Pounder-table", :table_status=> 0, :cluster => cluster)
    
    # Loop and add queries
    threads = []
    
    # Add new queries
    threads << Thread.new(table) { |blur_table|
      count = 0
      while count < 1000
        state = rand(3)
        completed = 20
        if state == 0 || state == 1
          completed = 20 - rand(20)
        end
        query = BlurQuery.create!(:query_string => "query_#{count}", :uuid => count, :created_at => Time.now, :updated_at => Time.now, :blur_table => blur_table, :state => state, :total_shards => 20, :complete_shards => completed, :times => '{}')
        puts "Query #{count} created: #{query.state_str}"
        count = count + 1
        sleep(rand(6))
      end
    }
    
    # Update running queries
    threads << Thread.new(table) { |blur_table|
      count = 0
      while count < 1000
        query = BlurQuery.where("state = 0 and blur_table_id = '#{blur_table.id}'").order("RAND()").first
        
        if query
          state = rand(3)
          completed = 20
          if state == 0 || state == 1
            completed = 20 - rand(20)
          end
          query.update_attributes!({:state => state, :complete_shards => completed, :updated_at => Time.now})
          puts "Query #{count} updated"
        end
        count = count + 1
        sleep(rand(6))
      end
    }
    
    threads.each { |thread| thread.join }
    
    # Clean up objects
    Zookeeper.destroy(zk.id)
  end
  
  def self.clean
    zk = Zookeeper.where("name = 'Pounder-zk'")
    if !zk.empty?
      zk.each do |z|
        Zookeeper.destroy(z.id)
      end
    end
  end
end