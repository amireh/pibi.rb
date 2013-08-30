module Pibi::AMQP
  module Entity
    include Pibi::Emitter
    include Pibi::Logger

    attr_reader :connection, :channel

    ExchangeDefaults = {
      durable: true,
      auto_delete: false,
      passive: false,
      nowait: false
    }

    # Connect to the AMQP broker.
    #
    # @async
    def connect(o)
      @connection = nil
      o = {
        'host' => 'localhost',
        'port' => 5672,
        'user' => 'guest',
        'pass' => 'guest',
        'vhost' => '/',
        'threaded' => true,
        'network_recovery_interval' => 5,
        'automatically_recover' => true
      }.merge(o || {})

      begin
        @connection = Bunny.new(o)
        @connection.start
      # just propagate the errors for now
      rescue Bunny::PossibleAuthenticationFailureException => e
        raise e
      rescue Bunny::TCPConnectionFailed => e
        raise e
      end

      yield(@connection) if block_given?

      emit :connected

      @connection
    end

    def connected?
      @connection.open?
    end

    # Disconnect from AMQP broker.
    def stop(&callback)
      unless @channel
        yield if block_given?
        return
      end

      log "disconnecting from broker"
      log "closing channel #{@channel.id}"

      if @channel.consumers.length > 1
        log "[warn] channel has #{@channel.consumers.length} consumers"
      end

      @channel.close
      @connection.close

      emit :stopped

      @connection = @channel = nil

      log "disconnected"

      yield if block_given?
    end

    # Open a new AMQP channel.
    #
    # @async
    def open_channel
      @channel = @connection.create_channel

      yield(@channel) if block_given?

      @channel
    end

    # Declare an exchange.
    def declare_exchange(name, type, options = {})
      options ||= {}
      options.merge! ExchangeDefaults

      log "declaring exchange #{name} of type #{type} with options:"
      log options

      e = @channel.send(type, name, options)

      yield(e) if block_given?

      e
    end

  end
end