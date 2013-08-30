describe Pibi::AMQP::Consumer do
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
      wait_for_amqp!
    end
  end

  it 'should invoke message handlers' do
    @consumer.stub(:on_message)
    @consumer.stub(:sweep_floor)

    @consumer.should_receive(:on_message)
    @consumer.should_receive(:sweep_floor)

    @consumer.start(amqp_settings) do
      @producer.queue('specs', 'sweep_floor', { client_id: 1 })

      wait_for_amqp!
    end
  end

  it 'should consume a message' do
    @consumer.stub(:on_message)
    @consumer.stub(:sweep_floor).and_return { true }

    @consumer.should_not receive(:on_message)
    @consumer.should_receive(:sweep_floor)

    @consumer.start(amqp_settings) do
      @producer.queue('specs', 'sweep_floor', { client_id: 1 })

      wait_for_amqp!
    end
  end

  it 'should reject a message missing an :id or a :client_id' do
    @consumer.stub(:on_message)
    @consumer.should_not receive(:on_message)

    @consumer.start(amqp_settings) do
      @producer.queue('specs', 'sweep_floor')
    end
  end

  it 'should consume a pending job' do
    @consumer.stub(:on_message)
    @consumer.should receive(:on_message).with(amqp_payload({
      id: 'sweep_floor',
      client_id: 1
    }))

    @consumer.start
    @consumer.stop
    @producer.queue('specs', 'sweep_floor', { client_id: 1 })
    @consumer.start(amqp_settings)
  end
end