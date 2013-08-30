module Pibi
  module RSpecHelpers
    module AMQP

      def amqp_settings
        {}
      end

      # module Pibi
      #   class Consumer
      #     # def connect(o)
      #     #   EM.next_tick do
      #     #     AMQP.connect do |connection|
      #     #       yield(connection) if block_given?
      #     #     end # AMQP connection
      #     #   end

      #     #   # sleep(1)
      #     # end
      #   end
      # end

      class SpecConsumer < Pibi::Consumer
        def initialize
          @exchange = {
            name: 'pibi.jobs',
            type: 'direct'
          }

          @queue = {
            name: 'specs'
          }
          super
        end
      end

      class SpecProducer < Pibi::Producer
        def initialize(*args)
          @exchange = {
            name: 'pibi.jobs',
            type: 'direct'
          }

          super(*args)
        end
      end

      def wait_for_amqp!(seconds = 2)
        sleep(seconds)
      end

      def amqp_payload(obj)
        JSON.parse(obj.to_json)
      end
    end
  end
end
