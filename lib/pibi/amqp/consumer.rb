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
  # AMQP message consumer.
  #
  # @note
  #   The worker class name must reflect the queue it will be consuming.
  class Consumer
    include Emitter

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
        log "already connected, ignoring request to #start"

        yield if block_given?

        return true
      end

      log "starting..."

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
              yield if block_given?

              emit :ready, e, queue

              queue.subscribe &method(:handle_payload)
            end # declaring queue
          end # declaring exchange
        end # opening channel
      end # connecting to broker
    end # starting the worker

    # Disconnect from AMQP broker.
    def stop(&callback)
      log "disconnecting from broker"

      if !ready?
        yield(self) if block_given?

        return
      end

      if !EM.reactor_running?
        log "EM reactor doesnt seem to be running, can't shut down"
        yield(self) if block_given?

        return
      end

      @connection && @connection.close do
        log "stopping"

        emit :stopped

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
    # @param message[:client_id] [Fixnum]
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

      EM.next_tick do
      # Pibi.start do
        AMQP.connect(connection_options) do |connection|
          connection.on_error &method(:on_connection_error)
          connection.on_tcp_connection_loss &method(:on_connection_loss)

          yield(connection) if block_given?
        end # AMQP connection
      end
    end

    def open_channel(connection)
      AMQP::Channel.new(connection) do |channel|
        channel.auto_recovery = true

        channel.on_error &method(:on_channel_error)

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
        routing_key: @queue[:name],
        nowait: true
      }.merge(@binding || {})

      if exchange.fanout?
        name = ''
        options[:auto_delete] = true
        options[:exclusive] = true
      end

      log "declaring queue '#{name}' with options: #{options}"

      channel.queue(name, options) do |q, declare_ok|
        log "binding queue to exchange '#{exchange.name}' with options: #{binding}"

        q.bind(exchange, binding) do
          log "all good, accepting work"

          yield(q) if block_given?
        end # binding queue
      end # declaring queue
    end

    def reject!(message, cause)
      log 'message rejected:', message

      raise cause if DEBUG

      message[:rejected] = true

      return false
    end

    def on_connection_error(connection, ec)
      puts "[error] AMQP connection-level exception:"
      puts
      puts dump_amqp_ec(ec)

      emit :amqp_connection_error, ec

      puts "Shutting down EM reactor, can not recover..."

      EM.stop
    end

    def on_connection_loss(connection, ec)
      puts '[error] AMQP TCP connection lost, trying to reconnect...'
      puts
      puts dump_amqp_ec(ec)

      emit :amqp_connection_lost, ec

      connection.reconnect(false, 2)
    end

    def on_channel_error(channel, ec)
      puts '[error] AMQP Channel error:'
      puts
      puts dump_amqp_ec(ec)

      emit :amqp_channel_error, ec.reply_text
    end

    def handle_payload(payload)
      event = nil

      begin
        event = JSON.parse(payload)
      rescue JSON::ParserError => e
        raise e if DEBUG

        puts "[error] bad payload from the API! message could not be deserialized:"
        puts "[error] raw payload: #{payload}"
        puts "[error] parser error: #{e}"

        return stop
      end

      event = event.with_indifferent_access

      if !event[:id] || !event[:client_id]
        return log "[error] message is missing a required field: #{event}"
      end

      log "got a message: #{event['id']}"

      # invoke the specific event handler, if any
      if event['id']
        method_id = event['id'].gsub('.', '_')

        if respond_to?(method_id)
          rc = send method_id, event

          # don't invoke the general message handler if the handler
          # returns truthy or has been #reject!-ed
          return if rc || event[:rejected]
        end
      end

      # invoke the general message handler
      log "  passing on to generic handler"
      on_message(event)
    end

    private

    def dump_amqp_ec(ec)
      <<-ERR
      AMQP class id : #{ec.class_id}
      AMQP method id: #{ec.method_id}
      Status code   : #{ec.reply_code}
      Error message : #{ec.reply_text}
      ERR
    end
  end
end