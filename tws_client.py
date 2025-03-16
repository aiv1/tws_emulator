import pandas as pd
import socket
import json
import threading

class TWSClient:
    def __init__(self, host='127.0.0.1', port=7498):
        self.host = host
        self.port = port
        self.socket = None
        self.connected = False
        self.running = False
        self.bar_callback = None
        self.order_status_callback = None
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.listen_thread = None

    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.connected = True
            self.running = True
            self.stop_event.clear()
            self.listen_thread = threading.Thread(target=self.listen, daemon=True)
            self.listen_thread.start()
            print(f"Connected to {self.host}:{self.port}")
        except Exception as e:
            print(f"Connection failed: {e}")
            self.connected = False
            self.running = False

    def disconnect(self):
        with self.lock:
            if self.connected:
                try:
                    self.socket.send((json.dumps({'type': 'disconnect'}) + '\n').encode('utf-8'))
                except Exception as e:
                    print(f"Disconnect send error: {e}")
                self.socket.close()
                self.connected = False
                self.running = False
                self.stop_event.set()
                print("Disconnected from emulator")
            else:
                print("Already disconnected")

    def req_real_time_bars(self, contract, bar_size, callback):
        self.bar_callback = callback
        self.send({'type': 'reqRealTimeBars', 'barSize': bar_size})
        print(f"Requested real-time bars with size {bar_size}")

    def place_order(self, contract, order):
        msg = {
            'type': 'placeOrder',
            'order': {'action': order.action, 'quantity': order.totalQuantity}
        }
        order_id = id(order)
        self.send(msg)
        trade = type('Trade', (), {
            'order': type('Order', (), {'orderId': order_id}),
            'orderStatus': type('OrderStatus', (), {
                'status': 'Filled',
                'avgFillPrice': 0.0  # Placeholder
            })
        })
        if self.order_status_callback:
            self.order_status_callback(trade)
        return order_id

    def send(self, msg):
        with self.lock:
            if self.connected:
                try:
                    self.socket.send((json.dumps(msg) + '\n').encode('utf-8'))
                    # print(f"Sent message: {msg}")
                except Exception as e:
                    print(f"Send error: {e}")
                    self.disconnect()

    def listen(self):
        buffer = ""
        while self.running:  # Use running to align with disconnect logic
            try:
                data = self.socket.recv(4096).decode('utf-8')
                if not data:
                    print("Socket closed by emulator")
                    self.disconnect()
                    break
                buffer += data
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if line.strip():
                        msg = json.loads(line)
                        # print(f"Received message: {msg}")
                        if msg['type'] == 'barUpdate' and self.bar_callback:
                            bar = type('Bar', (), {
                                'time': pd.to_datetime(msg['time']),
                                'open_': msg['open'],
                                'high': msg['high'],
                                'low': msg['low'],
                                'close': msg['close']
                            })
                            self.bar_callback(bar, True)
                        elif msg['type'] == 'orderStatus' and self.order_status_callback:
                            trade = type('Trade', (), {
                                'order': type('Order', (), {'orderId': msg['orderId']}),
                                'orderStatus': type('OrderStatus', (), {
                                    'status': msg['status'],
                                    'avgFillPrice': msg['avgFillPrice']
                                })
                            })
                            self.order_status_callback(trade)
                        elif msg['type'] == 'endOfData':
                            print("Received endOfData signal")
                            self.disconnect()
                            break  # Exit immediately after disconnect
            except json.JSONDecodeError as e:
                print(f"Client JSON error: {e} - Buffer: {buffer!r}")
            except Exception as e:
                print(f"Client listen error: {e}")
                self.disconnect()
                break
        print("Listen thread exiting")

    def set_order_status_callback(self, callback):
        self.order_status_callback = callback
        print("Order status callback set")

    def run(self):
        if not self.connected:
            print("Cannot run: Not connected")
            return
        print("Client running...")
        while self.running:
            self.stop_event.wait(timeout=1.0)  # Wait until stop_event is set
            if not self.running:
                break
        if self.listen_thread and self.listen_thread.is_alive():
            self.listen_thread.join(timeout=2.0)
            if self.listen_thread.is_alive():
                print("Warning: Listen thread did not exit cleanly")
        print("Client stopped")
        # Only disconnect if not already done
        with self.lock:
            if self.connected:
                self.disconnect()