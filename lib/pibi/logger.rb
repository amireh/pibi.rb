module Pibi
  module Logger
    def log(*msg)
      @id ||= parse_klass_id

      puts ">> [#{@id}]: #{msg.join(' ')}" if ENV['DEBUG']
    end

    def parse_klass_id
      self.class.to_s.gsub(/([a-z])([A-Z])/, '\1_\2').downcase
    end
  end
end