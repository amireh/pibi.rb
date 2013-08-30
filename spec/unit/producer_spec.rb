describe Pibi::AMQP::Producer do
  before(:each) do
    @consumer = SpecConsumer.new
    @producer = SpecProducer.new(amqp_settings)
  end

  after(:each) do
    @consumer.stop
    @producer.stop
  end

  it 'should publish a message' do
    exchange = {
      name: 'pibi.jobs',
      type: 'direct'
    }

    routing_key = 'specs'

    payload = {
      id: 'sweep_floor',
      client_id: 1
    }

    @consumer.stub(:on_message)
    @consumer.should_receive(:on_message).with amqp_payload(payload)

    @consumer.start(amqp_settings)
    @producer.send(:publish, exchange, routing_key, payload)

    wait_for_amqp!
  end

  it 'should queue a job' do
    @consumer.stub(:eat)
    @consumer.should_receive(:eat).with amqp_payload({
      id: 'eat',
      client_id: 1,
      food: 'Grilled Bananas'
    })

    @consumer.start do
      @producer.queue('specs', 'eat', {
        client_id: 1,
        food: 'Grilled Bananas'
      })
    end

    wait_for_amqp!
  end

  it 'should queue a job and be handled by a single worker' do
    consumer1 = @consumer
    consumer2 = SpecConsumer.new

    consumer1.stub(:on_message)
    consumer2.stub(:on_message)

    consumer1.should_receive(:on_message)
    consumer2.should_not receive(:on_message)

    consumer1.start
    consumer2.start

    @producer.queue('specs', 'eat', { client_id: 1, food: 'Grilled Bananas' })

    wait_for_amqp!

    consumer2.stop
  end

  context 'pushing notifications' do
    def configure(consumer)
      consumer.set_exchange('pibi.push', 'fanout')
      consumer.set_queue('', 'specs')
      consumer
    end

    before do
      configure(@consumer)
    end

    it 'should push a notification' do
      @consumer.stub(:eat)
      @consumer.should_receive(:eat).with amqp_payload({
        id: 'eat',
        client_id: 1,
        food: 'Grilled Bananas'
      })

      @consumer.start
      @producer.push('specs', 'eat', {
        client_id: 1,
        food: 'Grilled Bananas'
      })

      wait_for_amqp!
    end

    it 'should push a notification to all consumers' do
      consumer1 = @consumer
      consumer2 = configure SpecConsumer.new

      consumer1.stub(:on_message)
      consumer2.stub(:on_message)

      consumer1.should_receive(:on_message)
      consumer2.should_receive(:on_message)

      payload = {
        client_id: 1,
        food: 'Grilled Bananas'
      }

      consumer1.start
      consumer2.start

      @producer.push('specs', 'eat a spec', payload)

      wait_for_amqp!
    end

  end

end