module Pibi
  module Emitter
    def self.included(base)
      base.extend(ClassMethods)
    end

    def on(stage, &callback)
      @callbacks ||= {}
      @callbacks[stage.to_sym] ||= []
      @callbacks[stage.to_sym] << callback

      true
    end

    def emit(stage, *data)
      @callbacks ||= {}

      stage = stage.to_sym

      unless @callbacks.respond_to?(:[])
        raise 'Emitter: @callbacks is not a hash?' if DEBUG
        return false
      end

      (@callbacks[stage] || []).each { |callback|
        callback.call(*data)
      }
    end

    module ClassMethods
      def emits(events)
        original_method = instance_method(:initialize)

        define_method(:initialize) do |*args, &block|
          @callbacks ||= {}
          events.each { |event|
            @callbacks[event] = []
          }

          original_method.bind(self).call(*args, &block)
        end
      end
    end
  end
end