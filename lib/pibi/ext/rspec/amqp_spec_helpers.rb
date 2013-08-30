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
  module RSpecHelpers
    module AMQP
      def amqp_settings
        {}
      end

      class SpecConsumer < Pibi::AMQP::Consumer
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

      class SpecProducer < Pibi::AMQP::Producer
      end

      def wait_for_amqp!(seconds = 0.15)
        sleep(seconds)
      end

      def amqp_payload(obj)
        JSON.parse(obj.to_json)
      end
    end
  end
end
