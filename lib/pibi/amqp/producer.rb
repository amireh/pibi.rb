module Pibi
  class Producer < Consumer
    def initialize(amqp_config)
      @exchange = {
        name: 'amq.direct',
        type: 'direct'
      }
      @queue = {}
      @config = amqp_config

      super()
    end

    def queue(category, job, data = {}, &block)
      publish({
        exchange: {
          name: 'pibi.jobs',
          type: 'direct'
        },
        queue: {
          name: category
        },
        binding: {
          routing_key: category
        },
        payload: ( data || {} ).merge({ id: job })
      }, &block)
    end

    def push(notification_type, notification, data = {}, &block)
      publish({
        exchange: {
          name: 'pibi.push',
          type: 'fanout'
        },
        queue: {
          name: notification_type
        },
        binding: {
          routing_key: notification_type
        },
        payload: ( data || {} ).merge({ id: notification })
      }, &block)
    end

    def notify(*args)
      raise 'Producers can not dispatch notifications!'
    end

    protected

    def declare_queue(*args)
      yield(QueueSink) if block_given?
    end

    def publish(options, &block)
      @exchange.merge!(options[:exchange])
      @queue.merge!(options[:queue])
      @binding.merge!(options[:binding])

      if @exchange[:type] == 'fanout'
        @binding = {}
      end

      EM.next_tick do
        start(@config) do
          @exchange[:object].publish(options[:payload].to_json, @binding) do
            yield if block_given?
          end
        end
      end
    end

    private

    QueueSink = Object.new

    def QueueSink.subscribe(*args)
    end
  end
end