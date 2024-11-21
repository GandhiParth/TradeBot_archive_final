import time
import threading
from datetime import datetime, timedelta
import heapq

from typing import List


class DataHandler:
    def __init__(self, db_connection, num_threads: int = 3) -> None:
        """
        Intializes the DataHandler object

        Parameters:
        db_connection
        num_threads (int): Number of threads to use.
        """
        self.db_connection = db_connection  # Connection to TimescaleDB
        self.data_requirements = {}  # Stores data requirements per module
        self.last_sent_data = {}  # Tracks when the last data was sent to each module
        self.dispatch_queue = []  # Priority queue for module dispatching
        self.lock = threading.Lock()  # Lock for thread-safety
        self.running = True  # Flag to control thread running state
        self.num_threads = num_threads  # Number of worker threads
        self.threads = []  # To store thread references

    def _register_module(
        self, module: callable, stock_symbols: List[str], frequency: int
    ):
        """Register module to fetch specific data (stocks, frequency)."""
        self.data_requirements[module] = {
            "stock_symbols": stock_symbols,
            "frequency": frequency,  # Frequency in seconds
        }
        self.last_sent_data[module] = None  # Initialize last sent timestamp

        # Schedule the first dispatch time (current time)
        first_dispatch_time = datetime.now()
        with self.lock:
            heapq.heappush(self.dispatch_queue, (first_dispatch_time, module))

    def fetch_data(self, stock_symbols, table_name):
        """Fetch tick data from the database."""
        query = f"""
        SELECT * FROM {table_name}
        WHERE symbol IN ({', '.join([f"'{sym}'" for sym in stock_symbols])})
        ORDER BY timestamp DESC;
        """
        data = self.db_connection.execute(query)
        return data

    def send_data_to_module(self, module):
        """Send data to the module and reschedule its next dispatch."""
        with self.lock:
            requirements = self.data_requirements[module]
            stock_symbols = requirements["stock_symbols"]
            frequency = requirements["frequency"]

            # Fetch data and send it to the module
            data = self.fetch_data(stock_symbols)
            module.process_data(data)

            # Update the last sent time
            self.last_sent_data[module] = datetime.now()

            # Schedule the next dispatch
            next_dispatch_time = self.last_sent_data[module] + timedelta(
                seconds=frequency
            )
            heapq.heappush(self.dispatch_queue, (next_dispatch_time, module))

    def worker_thread(self):
        """Worker thread that continuously fetches modules from the dispatch queue."""
        while self.running:
            with self.lock:
                if not self.dispatch_queue:
                    continue  # No modules to dispatch

                # Get the module with the earliest dispatch time
                next_dispatch_time, module = heapq.heappop(self.dispatch_queue)

            # Calculate the time to wait until the next dispatch
            now = datetime.now()
            sleep_time = (next_dispatch_time - now).total_seconds()

            if sleep_time > 0:
                # Sleep until the next dispatch time
                time.sleep(sleep_time)

            # Send data to the module
            self.send_data_to_module(module)

    def run(self):
        """Start multiple worker threads to handle data dispatch."""
        for _ in range(self.num_threads):
            thread = threading.Thread(target=self.worker_thread)
            thread.start()
            self.threads.append(thread)

    def stop(self):
        """Gracefully stop all worker threads."""
        self.running = False
        for thread in self.threads:
            thread.join()
