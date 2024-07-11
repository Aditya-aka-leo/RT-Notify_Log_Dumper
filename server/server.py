from flask import Flask, request, jsonify
from src.utils.Kafka.producer import ProducerClient

producer = ProducerClient()

app = Flask(__name__)

@app.route('/api/data', methods=['POST'])
def handle_data():
    if request.is_json:
        data = request.get_json()
        print(data)
        try:
            producer.send('Channel-Aggregator', data)
            response = {"message": "Data sent to Kafka"}
            return jsonify(response), 200
        except Exception as e:
            response = {"error": str(e)}
            return jsonify(response), 500
    else:
        return jsonify({"error": "Request must be JSON"}), 400

if __name__ == '__main__':
    app.run(debug=True, port=8000,host='0.0.0.0')
