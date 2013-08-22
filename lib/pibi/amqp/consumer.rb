module Pibi
  # AMQP message consumer.
  #
  # @note
  #   The worker class name must reflect the queue it will be consuming.
  class Consumer
    def initialize(options = {})
      @id ||= self.class.to_s.gsub(/([a-z])([A-Z])/, '\1_\2').downcase

      @connection = nil
      @channel = nil

      [ 'exchange', 'queue' ].each do |required|
        if !self.instance_variable_get("@#{required}")
          raise 'Consumer is missing required config variable ' + required
        end
      end

      @exchange[:type] ||= 'direct'
      @exchange[:options] = {
        durable: true,
        auto_delete: false,
        passive: false,
        nowait: false
      }.merge(@exchange[:options] || {})

      @queue[:options] = {
        durable: true,
        auto_delete: false,
        passive: false,
        exclusive: false,
        nowait: false
      }.merge(@queue[:options] || {})

      @binding = {
        routing_key: @queue[:name],
        nowait: false
      }.merge(@binding || {})

      super()
    end

    def ready?
      @connection && @channel && @exchange[:object]
    end

    # Start accepting AMQP messages and handling jobs.
    def start(config = {}, &callback)
      if ready?
        return yield if block_given?
      end

      connect(config) do |connection|
        log "connected, opening channel..."
        @connection = connection

        open_channel(connection) do |channel|
          log "channel open, declaring exchange #{@exchange[:name]}..."
          @channel = channel

          declare_exchange(channel) do |e|
            log "exchange #{@exchange[:name]} declared, declaring queue #{@queue[:name]}..."
            @exchange[:object] = e

            declare_queue(channel, e) do |queue|
              callback.call if block_given?

              on_ready(e, queue)

              queue.subscribe do |payload|
                event = nil

                begin
                  event = JSON.parse(payload)
                rescue JSON::ParserError => e
                  raise e if DEBUG

                  puts "[error] bad payload from the API! message could not be deserialized:"
                  puts "[error] raw payload: #{payload}"
                  puts "[error] parser error: #{e}"

                  stop
                  next
                end

                if !event['id'] || !event['client']
                  log "[error] message is missing a required field: #{event}"
                  next
                end

                log "got a message: #{event['id']}"

                # invoke the specific event handler, if any
                if event['id']
                  method_id = event['id'].gsub('.', '_')

                  if respond_to?(method_id)
                    rc = send method_id, event

                    # don't invoke the general message handler if the handler
                    # returns truthy
                    next if rc
                  end
                end

                # invoke the general message handler
                log "  passing on to generic handler"
                on_message(event)

              end # handling payload
            end # declaring queue
          end # declaring exchange
        end # opening channel
      end # connecting to broker
    end # starting the worker

    # Disconnect from AMQP broker.
    def stop(&callback)
      log "disconnecting from broker"

      if !ready?
        return yield(self) if block_given?
      end

      on_stop

      if !EM.reactor_running?
        log "EM reactor doesnt seem to be running, can't shut down"
        yield(self) if block_given?

        return
      end

      @connection && @connection.close do
        log "stopping"

        yield(self) if block_given?
      end

      @connection = @channel = @exchange[:object] = @queue[:object] = nil
    end

    # Handle a message received from the API.
    #
    # @param message [Hash]
    #   The message from the API. Each message has its own structure,
    #   except for the following two fields:
    #
    # @param message[:id] [String]
    #   Unique job identifier.
    # @param message[:client] [Fixnum]
    #   The ID of the user for whom this job is being done.
    def on_message(message)
    end

    def set_exchange(name, type)
      @exchange[:name] = name
      @exchange[:type] = type
    end

    def set_queue(name, routing_key = '')
      @queue[:name] = name
      @binding[:routing_key] = routing_key
    end

    protected

    def log(*msg)
      puts ">> [#{@id}]: #{msg.join(' ')}" if ENV['DEBUG']
    end

    protected

    def connect(o)
      o = {
        'user' => 'guest',
        'password' => 'guest',
        'host' => 'localhost',
        'port' => 5672
      }.merge(o)

      connection_options = "amqp://#{o['user']}:#{o['password']}@#{o['host']}:#{o['port']}"

      # EM.next_tick do
        AMQP.connect(connection_options) do |connection|
          yield(connection) if block_given?
        end # AMQP connection
      # end
    end

    def open_channel(connection)
      AMQP::Channel.new(connection) do |channel|
        yield(channel) if block_given?
      end # AMQP channel
    end

    def declare_exchange(channel, options = {})
      o = @exchange.merge(options)

      channel.send(o[:type], o[:name], o[:options]) do |e, declare_ok|
        yield(e, declare_ok) if block_given?
      end # exchange
    end

    # Declare the queue used by this worker and bind it to the exchange.
    #
    # **1-to-N communication**
    #
    # When using a fanout exchange, the behaviour we desire is for every worker
    # to receive the same message and handle it, so:
    #
    # Each worker declares __their own queue__ with:
    #
    # - a name that is auto-generated by RabbitMQ (so @queue[:name] is ignored)
    # - bound to the exchange with a routing key that is @queue[:name], originally
    # - will be auto-deleted when the worker has stopped
    #
    # Mapping here is 1-to-N
    #
    # **1-to-1 communication**
    #
    # Direct and topic exchanges are different in that they process jobs by at
    # most one worker, so:
    #
    # - All workers bind to the __same queue__
    # - The queue name is the one specified in @queue[:name]
    # - Like in the fanout case, binds to the exchange using @queue[:name] as the
    #   routing key
    #
    # The queue must be durable and not auto-deleted, as it is shared by all
    # workers across the cluster.
    #
    # The mapping here is 1-to-1.
    def declare_queue(channel, exchange)
      name = @queue[:name]

      options = @queue[:options].clone
      binding = {
        routing_key: @queue[:name]
      }.merge(@binding || {})

      if exchange.fanout?
        name = ''
        options[:auto_delete] = true
        options[:exclusive] = true
      end

      q = channel.queue(name, options).bind(exchange, binding) do |success|
        unless success
          stop do
            raise RuntimeError, "Unable to declare queue."
          end
        end

        log "all good, accepting work"

        yield(q) if block_given?
      end # queue
    end

    def on_ready(exchange, queue)
    end

    def on_stop
    end
  end
end