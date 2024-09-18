from flask import Flask, jsonify, request
from confluent_kafka import Consumer, Producer, KafkaError

app = Flask(__name__)

# Kafka Consumer Configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Change to your Kafka broker
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'
}

# Kafka Producer Configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092'  # Change to your Kafka broker
}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)

consumer.subscribe(['pipeline_events'])  # Change to your Kafka topic

# Endpoint to consume Kafka messages
@app.route('/consume', methods=['GET'])
def consume_messages():
    messages = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for messages
            if msg is None:
                break  # Exit if no message is received
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    break
                else:
                    return jsonify({'error': msg.error().str()}), 500
            else:
                messages.append(msg.value().decode('utf-8'))

        return jsonify({'messages': messages})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Endpoint to produce/send messages to Kafka
@app.route('/produce', methods=['POST'])
def produce_message():
    try:
        # Get JSON data from request
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400

        # Convert the JSON to a string and send it to Kafka
        producer.produce('pipeline_events', value=str(data))

        # Ensure all messages are sent to Kafka
        producer.flush()

        return jsonify({'message': 'Message sent to Kafka'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
