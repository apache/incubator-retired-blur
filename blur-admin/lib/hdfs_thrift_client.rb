class HdfsThriftClient
  @@connections = {}

  def self.client(host_port)
#    if host_port.class == String
#      urls = host_port.split(/,/)
#    else
#      urls = Array(host_port)
#    end
#    blacklist_timeout = urls.size
#    blacklist_timeout = -1 if blacklist_timeout < 2
#    @@connections[urls] ||= ThriftClient.new ThriftHadoopFileSystem::Client, urls, :server_retry_period=> blacklist_timeout, :retries => 10
#    Client.new(@@connections[urls])
#
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
    
  private
    def pathname(path)
      ThriftHadoopFileSystem::Pathname.new(:pathname => path.to_s)
    end
  end
end
