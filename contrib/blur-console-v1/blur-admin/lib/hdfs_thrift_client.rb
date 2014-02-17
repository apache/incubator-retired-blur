# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

class HdfsThriftClient
  @@connections = {}

  def self.client(host_port)
    socket = Thrift::Socket.new(host_port.split(":").first,host_port.split(":").last)
    transport = Thrift::BufferedTransport.new(socket)
    transport.open
    protocol = Thrift::BinaryProtocol.new(transport)
    client = ThriftHadoopFileSystem::Client.new(protocol)
    
    Client.new(client)
  end

  class Client
    def initialize(client)
      @client = client
    end

    def stat(path)
      @client.stat(pathname(path))
    end

    def mkdirs(path)
      @client.mkdirs(pathname(path))
    end

    def rename(from, to)
      @client.rename(pathname(from),pathname(to))
    end

    def delete(path, recursive=false)
      @client.rm pathname(path), recursive
    end

    def put(local, remote)
      handle = @client.create(pathname(remote))
      Kernel.open(local) { |source|
        # read 1 MB at a time
        while record = source.read(1048576)
          @client.write(handle, record)
        end
        @client.close(handle)
      }
    end

    def ls(path, recursive=false)
      statuses = @client.listStatus(pathname(path))
      return statuses unless recursive
      statuses + statuses.select { |s| s.isdir }.map { |s| ls(s.path, recursive) }.flatten
    end
    
    def folder_tree(path, depth)
      if depth == 0
       return {:name => path, :size => folder_size(path)}
     end
      statuses = @client.listStatus(pathname(path))
      file_tree = {:name => path, :children =>
        statuses.map do |status|
          status.isdir ? folder_tree(status.path, depth - 1) : {:name => status.path, :size => status.length}
        end
      }
    end
    
  private
    def pathname(path)
      ThriftHadoopFileSystem::Pathname.new(:pathname => path.to_s)
    end
    
    def folder_size(path)
      statuses = @client.listStatus(pathname(path))
      size = 0
      statuses.each do |status|
        size += status.isdir ? folder_size(status.path) : status.length
      end
      size
    end
  end
end
