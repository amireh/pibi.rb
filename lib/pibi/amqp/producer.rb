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
  class Producer
    include Pibi::AMQP::Entity

    attr_accessor :connection_options

    def initialize(options = {}, autostart = true)
      connection_options = options

      start(options) if autostart

      super()
    end

    def start(options = connection_options)
      log "Connecting to AMQP broker..."
      connect(options) do
        log "Connected to AMQP broker. Opening channel..."

        open_channel do
          log "Channel #{@channel.id} open."
        end
      end
    end

    # Queue a job.
    def queue(category, job, data = {}, &block)
      publish({
        name: 'pibi.jobs',
        type: 'direct'
      }, category, (data || {}).merge({ id: job }), &block)
    end

    # Push a notification.
    def push(category, notification, data = {}, &block)
      publish({
        name: 'pibi.push',
        type: 'fanout'
      }, category, (data || {}).merge({ id: notification }), &block)
    end

    protected

    def publish(xdef, routing_key, payload = {}, &block)
      emit :before_publish, payload

      # the payload we'll be publishing
      unless payload = serialize_payload(payload)
        return false
      end

      # Grab a handle to the exchange
      e = declare_exchange(xdef[:name], xdef[:type])

      delivery = {
        routing_key: routing_key
      }

      # Don't use the routing key if it's a fanout; we need to reach all
      # consumers.
      delivery.delete(:routing_key) if e.type == :fanout

      log '' <<
        "publishing message #{payload} to '#{e.name}" <<
        (delivery.has_key?(:routing_key) ? "##{routing_key}" : "") <<
        "' (#{e.type})"

      # Deliver
      e.publish(payload, delivery)

      log "published"

      yield if block_given?

      emit :on_publish
    end # publish

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
  end
end