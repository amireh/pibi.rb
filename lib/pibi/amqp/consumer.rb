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
module Pibi::AMQP

  # AMQP message consumer.
  #
  # @note
  #   The worker class name must reflect the queue it will be consuming.
  class Consumer
    include Pibi::AMQP::Entity

    QueueDefaults = {
      durable: true,
      auto_delete: false,
      passive: false,
      exclusive: false,
      nowait: false
    }

    def initialize(options = {})
      @id ||= self.class.to_s.gsub(/([a-z])([A-Z])/, '\1_\2').downcase

      [ 'exchange', 'queue' ].each do |required|
        if !self.instance_variable_get("@#{required}")
          raise 'Consumer is missing required config variable ' + required
        end
      end

      unless @exchange[:name] || @exchange[:type]
        raise 'Consumer is missing an exchange :name or :type'
      end

      @exchange[:options] ||= {}

      @queue[:options] ||= {}
      @queue[:options].merge! QueueDefaults

      @binding = {
        routing_key: @queue[:name],
        nowait: false
      }.merge(@binding || {})

      on :stopped do
        @exchange[:object] = @queue[:object] = nil
      end

      super()
    end

    # Is the consumer ready to handle messages?
    def ready?
      @connection && @channel && @exchange[:object] && @queue[:object]
    end

    # Start accepting AMQP messages and handling jobs.
    def start(config = {}, &callback)
      if ready?
        log "already connected, ignoring request to #start"

        yield if block_given?

        return true
      end

      log "connecting to broker..."

      connect(config) do |connection|
        log "  opening channel..."

        open_channel do |channel|
          log "    channel #{channel.id} open, declaring exchange #{@exchange[:name]}..."

          declare_exchange(@exchange[:name], @exchange[:type], @exchange[:options]) do |e|
            @exchange[:object] = e
            log "      exchange #{@exchange[:name]} declared, declaring queue #{@queue[:name]}..."

            declare_queue(channel, e) do |queue|
              @queue[:object] = queue
              log "        queue declared and bound, ready to accept messages."

              yield if block_given?

              emit :ready, e, queue

              queue.subscribe &method(:handle_payload)
            end # declaring queue
          end # declaring exchange
        end # opening channel
      end # connecting to broker
    end # starting the worker

    def set_exchange(name, type)
      @exchange[:name] = name
      @exchange[:type] = type
    end

    def set_queue(name, routing_key = nil)
      @queue[:name] = name
      @binding[:routing_key] = routing_key if routing_key
    end

    protected

    # Handle a message received from the API.
    #
    # @param message [Hash]
    #   The message from the API. Each message has its own structure,
    #   except for the following two fields:
    #
    # @param message[:id] [String]
    #   Unique job identifier.
    # @param message[:client_id] [Fixnum]
    #   The ID of the user for whom this job is being done.
    def on_message(message)
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
        routing_key: name,
        nowait: true
      }.merge(@binding || {})

      if exchange.type == :fanout
        name = ''
        options[:auto_delete] = true
        options[:exclusive] = true
      end

      q = channel.queue(name, options)
      q.bind(exchange, binding)

      log "binding queue #{name} to exchange #{exchange.name} with options:"
      log binding

      yield(q) if block_given?
    end

    # Reject a message due to a missing field or any other reason.
    #
    # Rejected messages will not be passed to any handlers.
    def reject!(message, cause)
      log 'message rejected:', message

      raise cause if DEBUG

      message[:rejected] = true

      return false
    end

    # Parse the JSON payload and dispatch the message to a handler (if any) or
    # to the generic one (:on_message).
    #
    # All messages must specify the following keys:
    #
    #   - :id => String
    #   - :client_id => Fixnum
    def handle_payload(delivery, metadata, payload)
      message = nil

      begin
        message = JSON.parse(payload)
      rescue JSON::ParserError => e
        raise e if DEBUG

        puts "[error] bad payload from the API! message could not be deserialized:"
        puts "[error] raw payload: #{payload}"
        puts "[error] parser error: #{e}"

        return stop
      end

      message = message.with_indifferent_access

      if !message['id'] || !message['client_id']
        return log "[error] message is missing a required field: #{message}"
      end

      log "got a message: #{message['id']}"

      # invoke the specific message handler, if any
      method_id = message['id'].gsub('.', '_')

      if respond_to?(method_id)
        rc = send method_id, message

        # Don't invoke the general message handler if the handler
        # returns truthy or has been #reject!-ed
        return if rc || message[:rejected]
      end

      # invoke the general message handler
      log "  passing on to generic handler"
      on_message(message)
    end
  end
end