describe Pibi::Consumer do
  before(:each) do
    @consumer = SpecConsumer.new
    @producer = SpecProducer.new(amqp_settings)
  end

  after(:each) do
    @consumer.stop
    @producer.stop
  end

  it 'should invoke the generic message handler' do
    @consumer.stub(:on_message)
    @consumer.should_receive(:on_message)

    @consumer.start do
      @producer.queue('specs', 'sweep_floor', { client_id: 1 })
    end

    wait_for_amqp!
  end

  it 'should invoke message handlers' do
    @consumer.stub(:on_message)
    @consumer.stub(:sweep_floor)

    @consumer.should_receive(:on_message)
    @consumer.should_receive(:sweep_floor)

    @consumer.start(amqp_settings) do
      @producer.queue('specs', 'sweep_floor', { client_id: 1 })
    end

    wait_for_amqp!
  end

  it 'should consume a message' do
    @consumer.stub(:on_message)
    @consumer.stub(:sweep_floor).and_return { true }

    @consumer.should_receive(:sweep_floor)
    @consumer.should_not receive(:on_message)

    @consumer.start(amqp_settings) do
      @producer.queue('specs', 'sweep_floor', { client_id: 1 })
    end

    wait_for_amqp!
  end

  it 'should reject a message missing an :id or a :client_id' do
    @consumer.stub(:on_message)
    @consumer.should_not receive(:on_message)

    @consumer.start(amqp_settings) do
      @producer.queue('specs', 'sweep_floor')
    end

    wait_for_amqp!
  end

  it 'should consume a pending job' do
    @consumer.stub(:on_message)
    @consumer.should receive(:on_message)

    @consumer.start(amqp_settings) do
      @consumer.stop do
        sleep(1)

        @producer.queue('specs', 'sweep_floor', { client_id: 1 }) do
          @consumer.start(amqp_settings)
        end
      end
    end

    wait_for_amqp!(1.5)
  end
end