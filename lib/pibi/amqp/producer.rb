# Copyright (c) 2013 Algol Labs, LLC. <dev@algollabs.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
module Pibi
  class Producer < Consumer
    emits [ :before_publish, :on_publish  ]

    def initialize(amqp_config)
      @exchange = {
        name: 'amq.direct',
        type: 'direct'
      }
      @queue  = {}
      @config = amqp_config

      @publishing_lock = Mutex.new

      on :ready, &method(:on_ready)
      on :stopped, &method(:on_stopped)

      super()
    end

    def ready?
      @connection && @channel
    end

    # Establish a connection with the broker, but don't declare any entities
    # like exchanges or queues.
    #
    # @override
    def start(config = @config)
      if ready?

        if block_given?
          EM.next_tick do
            yield
          end
        end

        return true
      end

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

    def queue(category, job, data = {}, &block)
      publish({
        name: 'pibi.jobs',
        type: 'direct'
      }, category, (data || {}).merge({ id: job }), &block)
    end

    def push(category, notification, data = {}, &block)
      publish({
        name: 'pibi.push',
        type: 'fanout'
      }, category, (data || {}).merge({ id: notification }), &block)
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

      emit :before_publish, payload

      # the payload we'll be publishing
      unless payload = serialize_payload(payload)
        return false
      end

      exchange_definition = @exchange.merge(exchange_definition)

      start do
        # Grab a handle to the exchange
        declare_exchange(@channel, exchange_definition) do |e|
          log "publishing message #{payload} to '#{e.name}.#{routing_key}'"

          # Don't use the routing key if it's a fanout; we need to reach all
          # consumers.
          if e.fanout?
            routing_key = nil
          end

          # Deliver
          e.publish(payload, { routing_key: routing_key }) do
            yield if block_given?

            emit :on_publish
          end
        end # declaring the exchange
      end # connecting to the broker
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

    def on_stopped
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