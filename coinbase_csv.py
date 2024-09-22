import logging
import csv
import http.client
import json
from urllib.parse import urlencode, urlparse
from datetime import datetime, timedelta, UTC

logging.basicConfig(level=logging.DEBUG)  # Set logging to DEBUG to capture detailed logs


def generate_unique_csv_filename():
    """Generate a unique CSV filename based on the current datetime."""
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f'hourly_candles_{now}.csv'


def fetch_and_store_candles(product_id: str, granularity: int):
    # Coinbase API supports specific granularities, ensure it's valid
    if granularity not in [60, 300, 900, 3600, 21600, 86400]:
        logging.error(
            f"Invalid granularity: {granularity}. Allowed values are 60, 300, 900, 3600, 21600, 86400 seconds.")
        raise ValueError(f"Invalid granularity: {granularity}")

    base_url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"
    candle_data = []
    csv_filename = generate_unique_csv_filename()
    logging.info(f"CSV file will be saved as: {csv_filename}")

    # Start from today at midnight UTC
    end_time = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    logging.info(f"Starting data fetch from {end_time.isoformat()} (midnight UTC)")

    try:
        while True:
            start_time = end_time - timedelta(days=1)
            logging.info(f"Fetching candles from {start_time.isoformat()} to {end_time.isoformat()}")

            params = {
                'start': start_time.isoformat(),
                'end': end_time.isoformat(),
                'granularity': granularity  # Supported granularity values
            }

            # Build the full URL
            url_parts = urlparse(base_url)
            query_string = urlencode(params)
            full_url = f"{url_parts.path}?{query_string}"
            logging.debug(f"Built URL: {full_url}")

            conn = http.client.HTTPSConnection(url_parts.netloc)

            # Add User-Agent header along with Content-Type
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "bitcoin_hourly_price_fetcher/1.0"
            }
            logging.debug(f"Request headers: {headers}")

            try:
                logging.debug(f"Sending request to {url_parts.netloc}{full_url}")
                conn.request("GET", full_url, headers=headers)
                response = conn.getresponse()
                logging.info(f"Received response with status: {response.status} {response.reason}")
            except Exception as e:
                logging.error(f"Failed to send request: {e}")
                break

            # Log the full response body for debugging
            response_body = response.read().decode()
            logging.debug(f"Response body: {response_body}")

            if response.status != 200:
                raise Exception(f"Error fetching data: {response.status} {response.reason}")

            try:
                data = json.loads(response_body)
                logging.debug(f"Received data: {data}")
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding JSON response: {e}")
                break

            if not data:
                logging.info("No data available for this time period, stopping.")
                break

            # Append candle data to list
            for candle in data:
                close_timestamp = datetime.fromtimestamp(candle[0], UTC)
                candle_record = {
                    'Exchange': 'Coinbase',
                    'Pair': product_id,
                    'Close Timestamp': close_timestamp.isoformat(),
                    'Low': candle[1],
                    'High': candle[2],
                    'Open': candle[3],
                    'Close': candle[4],
                    'Volume': candle[5]
                }
                candle_data.append(candle_record)
            logging.info(f"Fetched {len(data)} candles for {end_time.isoformat()}")

            # Move to the previous day
            end_time = start_time
            logging.info(f"Moving to the previous day: {end_time.isoformat()}")

            # Stop if the date is before 2015-07-20
            if end_time < datetime(2015, 7, 20, tzinfo=UTC):
                logging.info("Reached the stopping date (2015-07-20), exiting.")
                break

    except Exception as e:
        logging.error(f"Error fetching data: {e}")

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt detected, writing data to CSV...")

    finally:
        # Once an error or interrupt occurs, write all the data to CSV
        if candle_data:
            logging.info(f"Writing {len(candle_data)} candle records to CSV")
            with open(csv_filename, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=['Exchange', 'Pair', 'Close Timestamp', 'Low', 'High', 'Open',
                                                          'Close', 'Volume'])
                writer.writeheader()
                writer.writerows(candle_data)
            logging.info(f"Successfully wrote {len(candle_data)} records to {csv_filename}")
        else:
            logging.info("No data to write.")


if __name__ == "__main__":
    try:
        logging.info("Starting the data fetch process.")
        fetch_and_store_candles('BTC-USD', 3600)  # 3600 seconds = 1 hour
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
