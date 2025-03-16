import socket
import threading
import pandas as pd
import time
import json
import argparse
from datetime import datetime
import pytz

# Timezone for consistency (matches your BACKTEST_TZ)
TZ = pytz.timezone("America/New_York")

class TWSEmulator:
    def __init__(self, host='127.0.0.1', port=7498, data_file=None):
        self.host = host
        self.port = port
        self.data_file = data_file
        self.data_df = None
        self.clients = []
        self.orders = {}
        self.order_id = 0
        self.running = False
        self.server_socket = None

    def load_data(self):
        """Load data from .pkl file and prepare it for streaming."""
        if not self.data_file:
            raise ValueError("Data file path is required")
        self.data_df = pd.read_pickle(self.data_file)
        self.data_df['date'] = pd.to_datetime(self.data_df['date'])
        # Localize to TZ if naive
        if self.data_df['date'].dt.tz is None:
            self.data_df['date'] = self.data_df['date'].dt.tz_localize(TZ)
        print(f"Loaded {len(self.data_df)} bars from {self.data_file}")

    def aggregate_to_seconds(self, target_freq='5s'):
        """Aggregate data to 5-second bars if not already in that frequency."""
        time_diffs = self.data_df['date'].diff().dt.total_seconds().dropna()
        freq_mode = time_diffs.mode()[0] if not time_diffs.empty else 60
        if abs(freq_mode - 5) < 2:  # Already ~5 seconds
            return self.data_df
        elif abs(freq_mode - 60) < 10:  # ~1 minute, resample to 5 seconds
            result_df = self.data_df.resample(target_freq, on='date').agg({
                'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
            }).dropna().reset_index()
            print(f"Resampled from ~{freq_mode}s to 5s: {len(result_df)} bars")
            return result_df
        else:
            raise ValueError(f"Unsupported frequency: {freq_mode} seconds")

    def start_server(self):
        """Start the TCP server to accept client connections."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"TWS Emulator listening on {self.host}:{self.port}")
        self.running = True
        threading.Thread(target=self.accept_clients, daemon=True).start()

    def accept_clients(self):
        """Accept incoming client connections."""
        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                print(f"Client connected from {addr}")
                self.clients.append(client_socket)
                threading.Thread(target=self.handle_client, args=(client_socket,), daemon=True).start()
            except Exception as e:
                if self.running:
                    print(f"Error accepting client: {e}")

    def handle_client(self, client_socket):
        """Handle messages from a connected client."""
        while self.running:
            try:
                data = client_socket.recv(4096).decode('utf-8')
                if not data:
                    self.clients.remove(client_socket)
                    client_socket.close()
                    break
                self.process_message(client_socket, data)
            except Exception as e:
                print(f"Error handling client: {e}")
                break

    def process_message(self, client_socket, message):
        """Process incoming messages (e.g., bar requests, orders)."""
        try:
            msg = json.loads(message)
            msg_type = msg.get('type')

            if msg_type == 'reqRealTimeBars':
                # Start streaming bars
                threading.Thread(target=self.stream_bars, args=(client_socket,), daemon=True).start()
            elif msg_type == 'placeOrder':
                self.handle_order(client_socket, msg)
            elif msg_type == 'disconnect':
                self.clients.remove(client_socket)
                client_socket.close()
        except json.JSONDecodeError:
            print(f"Invalid message: {message}")

    def stream_bars(self, client_socket):
        df = self.aggregate_to_seconds('5s')
        for idx, row in df.iterrows():
            if not self.running:
                break
            bar = {'type': 'barUpdate', 'time': row['date'].isoformat(), 'open': row['open'], 'high': row['high'], 'low': row['low'], 'close': row['close']}
            print(f"Streaming bar: {bar['time']}")
            self.send_message(client_socket, bar)
            time.sleep(0.01)  # Simulate real-time
        # Send end-of-data message
        self.send_message(client_socket, {'type': 'endOfData'})
        print("Finished streaming bars")

    def handle_order(self, client_socket, msg):
        """Simulate order placement and fill."""
        self.order_id += 1
        order = msg.get('order')
        action = order.get('action')  # 'BUY' or 'SELL'
        quantity = order.get('quantity')

        # Simulate fill at next bar's open price (assuming streaming is in progress)
        # For simplicity, use the last sent bar's close; enhance to track next bar
        fill_price = self.data_df['close'].iloc[min(self.order_id, len(self.data_df)-1)]
        
        # Send order status update
        status = {
            'type': 'orderStatus',
            'orderId': self.order_id,
            'status': 'Filled',
            'avgFillPrice': float(fill_price)  # Ensure JSON-serializable
        }
        self.orders[self.order_id] = {'action': action, 'quantity': quantity, 'fill_price': fill_price}
        self.send_message(client_socket, status)

    def send_message(self, client_socket, msg):
        """Send a JSON message to the client."""
        try:
            client_socket.send((json.dumps(msg) + '\n').encode('utf-8'))
        except Exception as e:
            print(f"Error sending message: {e}")

    def stop(self):
        """Stop the emulator."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        for client in self.clients:
            client.close()
        print("TWS Emulator stopped")

    def run(self):
        """Main run loop."""
        self.load_data()
        self.start_server()
        try:
            while True:
                time.sleep(1)  # Keep main thread alive
        except KeyboardInterrupt:
            self.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TWS API Emulator")
    parser.add_argument('--data-file', type=str, required=True, help="Path to .pkl data file")
    args = parser.parse_args()

    emulator = TWSEmulator(data_file=args.data_file)
    emulator.run()

""" 
& C:/Users/mk/miniforge3/envs/torch_env/python.exe c:/Users/mk/Downloads/dev/ib_volatile/tws_emulator.py --data-file "../data/ohlcv_1min_1D_nvda.pkl"

 """