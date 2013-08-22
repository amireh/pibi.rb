module Pibi
  class Producer < Consumer
    def initialize(amqp_config)
      @exchange = {
        name: 'amq.direct',
        type: 'direct'
      }
      @queue  = {}
      @config = amqp_config

      @publishing_lock = Mutex.new
      @callbacks ||= {}

      super()
    end

    def ready?
      @connection && @channel
    end

    def queue(category, job, data = {}, &block)
      publish({
        name: 'pibi.jobs',
        type: 'direct'
      }, category, (data || {}).merge({ id: job }), &block)
      # publish({
      #   exchange: {
      #     name: 'pibi.jobs',
      #     type: 'direct'
      #   },
      #   queue: {
      #     name: category
      #   },
      #   binding: {
      #     routing_key: category
      #   },
      #   payload: ( data || {} ).merge({ id: job })
      # }, &block)
    end

    def push(category, notification, data = {}, &block)
      publish({
        name: 'pibi.push',
        type: 'fanout'
      }, category, (data || {}).merge({ id: notification }), &block)
      # publish({
      #   exchange: {
      #     name: 'pibi.push',
      #     type: 'fanout'
      #   },
      #   queue: {
      #     name: notification_type
      #   },
      #   binding: {
      #     routing_key: notification_type
      #   },
      #   payload: ( data || {} ).merge({ id: notification })
      # }, &block)
    end

    protected

    def lock(&callback)
      @broadcast_lock.synchronize do
        yield self if block_given?
      end
    end

    def publish(exchange_definition, routing_key, payload = {}, &block)
      # if !ready?
      #   return publish_later(options, &block)
      # end

      # the payload we'll be publishing
      unless payload = serialize_payload(payload)
        return false
      end

      exchange_definition = @exchange.merge(exchange_definition)

      # EM.next_tick do
        start do
          # Grab a handle to the exchange
          declare_exchange(@channel, exchange_definition) do |e|
            # Don't use the routing key if it's a fanout; we need to reach all
            # consumers.
            if e.fanout?
              routing_key = nil
            end

            # Deliver
            log "publishing message #{payload} to '#{routing_key}'"
            e.publish(payload, { routing_key: routing_key }) do
              yield if block_given?

              (@callbacks[:on_publish] || []).map(&:call)
            end
          end # declaring the exchange
        end # connecting to the broker
      # end # deferring into the EM loop
    end # publish

    def publish_later(options, &block)
      lock do
        @queued << {
          options: options,
          callback: block
        }

        log "message queued until connection to the broker has been established"
      end

      false
    end

    # Establish a connection with the broker, but don't declare any entities
    # like exchanges or queues.
    #
    # @override
    def start(config = @config)
      connect(config) do |connection|
        @connection = connection

        log "connected, opening channel..."

        open_channel(connection) do |channel|
          @channel = channel

          log "channel open, declaring exchange #{@exchange[:name]}..."

          yield if block_given?
        end
      end
    end

    # No-op for a producer as we won't be consuming any queue.
    #
    # @override
    def declare_queue(*args)
      yield(QueueSink) if block_given?
    end

    def on_ready(e, q)
      @queued.each { |message|
        publish message[:options], &message[:callback]
      }

      lock do
        @queued = []
      end
    end

    def on_stop
      @queued = []
    end

    # Serialize the payload and guard against any malformations
    def serialize_payload(hash)
      begin
        hash.to_json
      rescue JSON::NestingError => e
        log "[error] bad message to publish:"
        log "[error] payload: #{options[:payload]}"
        log "[error] error: #{e}"

        raise e if DEBUG

        return false
      rescue Exception => e
        log "[error] unable to serialize message payload:"
        log "[error] payload: #{options[:payload]}"
        log "[error] error: #{e}"

        raise e if DEBUG

        return false
      end
    end

    private

    QueueSink = Object.new

    def QueueSink.subscribe(*args)
    end
  end
end