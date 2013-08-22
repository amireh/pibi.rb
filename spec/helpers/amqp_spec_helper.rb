def amqp_settings
  {}
end

module Pibi
  class Consumer
    def connect(o)
      AMQP.connect do |connection|
        yield(connection) if block_given?
      end # AMQP connection
    end
  end
end