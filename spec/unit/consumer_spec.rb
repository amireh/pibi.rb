describe Pibi::Consumer do
  class SpecWorker < Pibi::Consumer
    def initialize
      @exchange = {
        name: 'pibi.jobs',
        type: 'direct'
      }

      @queue = {
        name: 'tasks'
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

  before(:each) do
    @consumer = SpecWorker.new
    @producer = SpecProducer.new(amqp_settings)
  end

  after(:each) do
    @consumer.stop
  end

  it 'should invoke the generic message handler' do
    @consumer.stub(:on_message)

    @consumer.start(amqp_settings) do
      @producer.queue('tasks', 'sweep_floor', { client: 1 }) do
        @consumer.should_receive(:on_message)
      end
    end

    @consumer.stop
  end

  it 'should invoke message handlers' do
    @consumer.stub(:on_message)
    @consumer.stub(:sweep_floor)

    @consumer.start(amqp_settings) do
      @producer.queue('tasks', 'sweep_floor', { client: 1 }) do
        @consumer.should_receive(:on_message)
        @consumer.should_receive(:sweep_floor)
      end
    end
  end

  it 'should consume a message' do
    @consumer.stub(:on_message)
    @consumer.stub(:sweep_floor).and_return { true }

    @consumer.start(amqp_settings) do
      @producer.queue('tasks', 'sweep_floor', { client: 1 }) do
        @consumer.should_receive(:sweep_floor)
        @consumer.should not_receive(:on_message)
      end
    end
  end

  it 'should reject a message missing an :id or a :client' do
    @consumer.stub(:on_message)
    @consumer.start(amqp_settings) do
      @producer.queue('tasks', 'sweep_floor') do
        @consumer.should_not receive(:on_message)
      end
    end

    @consumer.stop
  end
end