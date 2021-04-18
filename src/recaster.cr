# TODO: Write documentation for `SimpleUdpStreamRecaster`
require "yaml"
require "uri"
require "socket"

class URI
  def self.new(ctx : YAML::ParseContext, node : YAML::Nodes::Node)
    parse(String.new(ctx, node))
  end
end

module SimpleUdpStreamRecaster
  VERSION = "0.1.0"

  alias Config = Hash(UInt16, Array(URI))

  class Server
    def initialize(port : UInt16)
      @targets = [] of Socket::IPAddress
      @server = UDPSocket.new
      @server.bind "0.0.0.0", port
      start_listening
    end

    def close
      @server.close
    end

    def targets=(targets : Array(URI))
      @targets = targets.map do |uri|
        raise "we accept UDP only : #{uri}" if uri.scheme != "udp"
        raise "host and port is required : #{uri}" if uri.host.nil? || uri.port.nil?
        puts "updating target for #{@server.local_address.port}: #{uri}"
        Socket::IPAddress.new uri.host.not_nil!, uri.port.not_nil!
      end
    end

    def start_listening
      spawn do
        until @server.closed?
          begin
            message, client_addr = @server.receive 1464
            @targets.each do |target|
              @server.send message, target
            end
          rescue IO::Error | Socket::Error
          rescue ex
            puts ex.inspect
          end
        end
      end
    end
  end

  class RecasterManager
    def initialize(conf = "config.yml")
      @conf_file = conf
      @conf = Config.new
      @servers = Hash(UInt16, Server).new

      load_config
    end

    def load_config
      @conf = Config.from_yaml File.read(@conf_file)
      new = @conf.keys
      old = @servers.keys

      (old - new).each do |port|
        @servers[port].close
        @servers.delete port
        puts "Removed server #{port}"
      end

      (new - old).each do |port|
        server = Server.new port
        @servers[port] = server
        puts "Started server #{port}"
      end

      @servers.each do |port, server|
        server.targets = @conf[port]? || [] of URI
      end
    rescue ex
      puts "Failed to load config"
      puts ex.inspect
    end
  end

  def self.start_server
    manager = RecasterManager.new

    Signal::HUP.trap do
      puts "Recived HUP, reloading config"
      manager.load_config
    end

    sleep
  end
end

SimpleUdpStreamRecaster.start_server
