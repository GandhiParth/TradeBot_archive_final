import concurrent.futures
import logging
import time
from datetime import datetime, timedelta

import polars as pl
from exceptions import KiteLoginError, MissingKeyError, MissingSectionError
from kiteconnect import KiteConnect, KiteTicker
from pyotp import TOTP
from ratelimit import limits, sleep_and_retry
from selenium import webdriver  # v 4.18.1
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from utils.utils import create_update_ini_file, get_today, read_ini_file

from typing import List, Dict, Optional, Literal, Tuple, Union, Any

# contextmanager


logger = logging.getLogger(__name__)


class KiteLogin:
    """
    Handles Connection to Kite Broker
    """

    def __init__(self, credentials_path: str):
        """
        Initializes KiteLogin with credentials loaded from an ini file.

        The file must contain a "Kite" section with the following keys:
        - "user_id"
        - "password"
        - "api_key"
        - "api_secret_key"
        - "totp_key"
        - "file_location" : saved as kite_tokens_{today_date}.ini

        Parameters:
        credentials_path (str): The path to the credentials file (must be an ini file)
        """

        self._credentials = read_ini_file(file_location=credentials_path)

        self._check_file()

    def _check_file(self) -> None:
        """
        Validate the credentials file.

        This method checks that the ini file has the required "Kite" section
        and that all necessary keys are present within that section.

        Raises:
        MissingSectionError: If the "Kite" section is missing from the ini file.
        MissingKeyError: If any of the required keys are missing in the "Kite" section.

        """
        if "Kite" not in self._credentials.sections():
            raise MissingSectionError(
                """The "Kite" section is missing from the ini file"""
            )
        required_keys = [
            "user_id",
            "password",
            "api_key",
            "api_secret_key",
            "totp_key",
            "file_location",
        ]

        for key in required_keys:
            if key not in self._credentials["Kite"]:
                raise MissingKeyError(
                    f"""The required key "{key}" is missing in the 'Kite' section."""
                )

    def _generate_request_token(self) -> str:
        """
        Generate a request token for KiteConnect.

        Returns:
        str: The generated request token.
        """
        kite = KiteConnect(api_key=self._credentials["Kite"]["api_key"])
        options = FirefoxOptions()
        options.add_argument("--headless")
        driver = webdriver.Firefox(options=options)
        driver.get(kite.login_url())
        driver.implicitly_wait(10)
        username = driver.find_element(By.ID, "userid")
        password = driver.find_element(By.ID, "password")
        username.send_keys(self._credentials["Kite"]["user_id"])
        password.send_keys(self._credentials["Kite"]["password"])
        driver.find_element(
            By.XPATH, "//button[@class='button-orange wide' and @type='submit']"
        ).send_keys(Keys.ENTER)
        pin = driver.find_element(By.XPATH, '//*[@type="number"]')
        token = TOTP(self._credentials["Kite"]["totp_key"]).now()
        pin.send_keys(token)
        time.sleep(10)
        request_token = driver.current_url.split("request_token=")[1][32]
        driver.quit()
        return request_token

    def _generate_access_token(self, request_token: str) -> str:
        """
        Generate an access token for the given request_token.

        Parameters:
        request_token (str): The request token generated during login.

        Returns:
        str: The generated access token.

        Raises:
        KiteLoginError: If access token generation is not successful.
        """
        kite = KiteConnect(api_key=self._credentials["Kite"]["api_key"])
        kite.invalidate_access_token()
        response = kite.generate_session(
            request_token=request_token,
            api_secret=self._credentials["Kite"]["api_secret_key"],
        )

        if response["status"] != "success":
            raise KiteLoginError("Kite Access Token Generation Not Successful")

        access_token = response["data"]["access_token"]
        return access_token

    def _auto_login(self) -> KiteConnect:
        """
        Automatically logs in to Kite and returns a KiteConnect object.

        Returns:
        KiteConnect: The KiteConnect object after successful login.
        """
        request_token = self._generate_request_token()
        access_token = self._generate_access_token(request_token=request_token)

        today = get_today()
        file_location = (
            f"""{self._credentials["Kite"]["file_location"]}kite_tokens_{today}.ini"""
        )

        logger.info(f"""Kite Tokens saved at location {file_location}""")

        create_update_ini_file(
            file_location=file_location,
            section_key="Kite",
            data={"request_token": request_token, "access_token": access_token},
        )

        try:
            kite = KiteConnect(
                api_key=self._credentials["Kite"]["api_key"], access_token=access_token
            )
            logger.info("Kite Connection object created successfully")

            return kite
        except Exception as e:
            logger.error(e)
            raise ValueError(e)

    def auto_login(self) -> KiteConnect:
        """
        Automatically logs in to Kite and returns a KiteConnect object.

        Returns:
        KiteConnect: The KiteConnect object after loading credentials or performing login.
        """
        today = get_today()
        file_location = (
            f"""{self._credentials["Kite"]["file_location"]}Kite_tokens_{today}.ini"""
        )

        credentials = read_ini_file(file_location=file_location)

        if credentials == None:
            return self._auto_login()

        kite = KiteConnect(
            api_key=self._credentials["Kite"]["api_key"],
            access_token=credentials["Kite"]["access_token"],
        )
        logger.info(
            f"""Kite Connection object created succesfully from loading credentials at {file_location}"""
        )
        return kite


class KiteHistorical:
    """
    Gets Historical Data from Kite
    """

    def __init__(
        self, kite: KiteConnect, file_location: str, config_location: str
    ) -> None:
        """
        Initializes the KiteHistorical class.

        Parameters:
        kite (KiteConnect): An instance of the KiteConnect object used for making API requests.
        file_location (str): Path to the instrument list CSV file.
        config_location (str): Path to the Kite INI configuration file.
        """
        self._kite = kite
        self._file_location = file_location

        self._config_location = config_location
        self._config = read_ini_file(self._config_location)
        self._historical_rate_limit: Dict[str, int] = self._config["KiteConfig"][
            "historical_data_limit"
        ]

        self._historical_api_limit = self._config["KiteConfig"]["api_rate_limit"][
            "historical"
        ]

        self._connection_string = self._config["KiteTables"]["historical"]

    def _get_instrument_token_from_file(
        self, file_location: str
    ) -> List[Tuple[str, str]]:
        """
        Parses the instrument file and returns a list of symbol and instrument token pairs.

        Parameters:
        file_location (str): Path to the CSV file containing instrument data.

        Returns:
        List[Tuple[str, str]]: A list of tuples where each tuple contains a symbol and its corresponding instrument token.
        """
        raise NotImplementedError

    def _get_date_ranges(
        self,
        from_date: datetime,
        to_date: datetime,
        interval: Literal[
            "minute",
            "3minute",
            "5minute",
            "10minute",
            "15minute",
            "30minute",
            "60minute",
        ],
    ) -> List[Tuple[datetime, datetime]]:
        """
        Divides the time period into date ranges that comply with the rate limit for the specified interval.

        Parameters:
        from_date (datetime): Start date for fetching historical data.
        to_date (datetime): End date for fetching historical data.
        interval (Literal["minute", "3minute", "5minute", "10minute", "15minute", "30minute", "60minute"]): Frequency of the historical data.

        Returns:
        List[Tuple[datetime, datetime]]: A list of tuples representing the start and end dates for each range.
        """
        if interval not in self._historical_rate_limit:
            raise ValueError(
                "Invalid interval. Must be one of: "
                + ", ".join(self._historical_rate_limit.keys())
            )

        max_days = self._historical_rate_limit[interval]

        current_start = from_date
        date_ranges = []

        while current_start < to_date:
            current_end = min(current_start + timedelta(days=max_days), to_date)
            date_ranges.append((current_start, current_end))
            current_start = current_end + timedelta(seconds=1)

        return date_ranges

    @limits(calls=3, period=1)
    @sleep_and_retry
    def _get_historical_data(
        self,
        instrument_token: str,
        from_date: datetime,
        to_date: datetime,
        interval: str,
        continuous=False,
        oi=False,
    ) -> Dict[str, Union[str, Dict[str, List[List[Union[str, float, int]]]]]]:
        """
        Fetches historical data from the Kite API.

        Parameters:
        instrument_token (str): The token of the instrument for which data is to be fetched.
        from_date (datetime): Start date for the data.
        to_date (datetime): End date for the data.
        interval (str): Frequency of the data (e.g., "minute", "day").
        continuous (bool, optional): If True, fetch continuous contract data for futures. Defaults to False.
        oi (bool, optional): If True, fetch open interest data. Defaults to False.

        Returns:
        A dictionary with the following structure:
            {
                "status": "success" | "failure",
                "data": {
                    "candles": [
                        [
                            timestamp (str in ISO 8601 format, e.g. "2017-12-15T09:15:00+0530"),
                            open (float),
                            high (float),
                            low (float),
                            close (float),
                            volume (int)
                        ],
                        ...
                    ]
                }
        }
        """
        data = self._kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval=interval,
            continuous=continuous,
            oi=oi,
        )
        return data

    def _get_data(
        self,
        date_ranges: List[Tuple[datetime, datetime]],
        instrument_token: str,
        interval: str,
        config: Dict[str, bool] = {"continous": False, "oi": False},
    ) -> Tuple[List, Dict]:
        """
        Concurrently fetches historical data for a specific instrument over multiple date ranges.

        Parameters:
        date_ranges (List[Tuple[datetime, datetime]]): List of start and end date ranges.
        instrument_token (str): The token of the instrument for which data is to be fetched.
        interval (str): Frequency of the data (e.g., "minute", "day").
        config (Dict[str, bool], optional): Configuration dictionary for options like "continuous" and "oi".

        Returns:
        Tuple[List, Dict]: A tuple containing the fetched data and a dictionary of failed date ranges.
        """
        continuous = config.get("continuous")
        oi = config.get("oi")

        param_map = {}
        all_data = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._historical_api_limit
        ) as executor:
            futures = []
            for start_date, end_date in date_ranges:
                future = executor.submit(
                    self._get_historical_data,
                    instrument_token,
                    start_date,
                    end_date,
                    interval,
                    continuous,
                    oi,
                )
                futures.append(future)
                param_map[future] = (
                    instrument_token,
                    start_date,
                    end_date,
                    interval,
                    continuous,
                    oi,
                )

            for future in concurrent.futures.as_completed(futures):
                try:
                    response = future.result()
                    if response["status"] == "success":
                        all_data.extend(response["data"]["candles"])
                        del param_map[future]
                    else:
                        logger.info(f"""Failed: {param_map[future]}""")

                except Exception as e:
                    raise e
        return all_data, param_map

    def _generate_dataframe(
        self,
        candles: List[List[str, float, float, float, float, Optional[float]]],
        symbol: str,
    ) -> pl.DataFrame:
        """
        Generates a Polars DataFrame from the given OHLCV candle data.

        Parameters:
        candles (List[List[str, float, float, float, float, float, Optional[float]]]): List of OHLCV candle data.
        symbol (str): The trading symbol for the instrument.

        Returns:
        pl.DataFrame: A Polars DataFrame containing the historical data.
        """
        return (
            pl.DataFrame(
                candles,
                schema=[
                    ("timestamp", pl.Utf8),
                    ("open", pl.Float64),
                    ("high", pl.Float64),
                    ("low", pl.Float64),
                    ("close", pl.Float64),
                    ("volume", pl.Int64),
                ],
                orient="row",
            )
            .lazy()
            .with_columns(
                pl.lit("EQ").alias("instrument_type"),
                pl.lit(symbol).alias("trading_symbol"),
                pl.col("timestamp").str.to_date("%Y-%m-%dT%H:%M:%S%z").alias("date"),
                pl.col("timestamp").str.to_time("%Y-%m-%dT%H:%M:%S%z").alias("time"),
                pl.col("timestamp")
                .str.to_datetime("%Y-%m-%dT%H:%M:%S%z")
                .dt.convert_time_zone(time_zone="Asia/Calcutta")
                .alias("datetime"),
            )
            .drop("timestamp")
            .select(
                "instrument_type",
                "trading_symbol",
                "datetime",
                "date",
                "time",
                "open",
                "high",
                "low",
                "close",
                "volume",
            )
            .sort("datetime", descending=True)
        ).collect()

    def _retry_failed_date_ranges(self, param_map):
        """
        Retries fetching data for the date ranges that failed in the initial request.

        Parameters:
        param_map (Dict): A dictionary of failed date ranges and their corresponding parameters.
        """
        raise NotImplementedError

    def get_historical_data(
        self,
        file_location: str,
        start_date: datetime,
        end_date: datetime,
        frequency: Literal[
            "minute",
            "3minute",
            "5minute",
            "10minute",
            "15minute",
            "30minute",
            "60minute",
            "day",
        ],
        table_name: str,
    ) -> None:
        """
        Fetches historical data for all instruments listed in the provided CSV file and writes the data to a database table.

        Parameters:
        file_location (str): Path to the CSV file containing the symbols to fetch historical data for.
        start_date (datetime): Start date for fetching historical data.
        end_date (datetime): End date for fetching historical data.
        frequency (Literal["minute", "3minute", "5minute", "10minute", "15minute", "30minute", "60minute", "day"]): Frequency of the historical data.
        table_name (str): Name of the table where the data will be stored.
        """
        symbol_tokens = self._get_instrument_token_from_file(
            file_location=file_location
        )

        date_ranges = self._get_date_ranges(
            from_date=start_date, to_date=end_date, interval=frequency
        )

        for symbol, token in symbol_tokens:
            data, param_map = self._get_data(
                date_ranges=date_ranges, instrument_token=token
            )
            logger.info(f"""Data Received Successfully for {token}""")
            logger.info(
                f"""Failed to get for {len(param_map)} date ranges for {symbol}"""
            )

            df = self._generate_dataframe(candles=data, symbol=symbol)
            df.write_database(
                table_name=table_name,
                connection=self._connection_string,
                if_table_exists="append",
            )

            self._retry_failed_date_ranges(self, param_map)

    def sync_tables(self):
        """ "
        Syncs all the historical data tables to the current date.
        """
        raise NotImplementedError


def kite_map_symbols_tokens(
    instrument_list: pl.DataFrame, file_location: str
) -> pl.DataFrame:
    """
    Maps the symbols and to the instrument tokens
    """
    raise NotImplementedError


# KiteTicker Functions


def on_ticks(ws: Any, ticks: List[Dict[str, Any]]) -> None:
    """
    Triggered when ticks are received.

    Parameters:
    ws (Any): The WebSocket object.
    ticks (List[Dict[str, Any]]): List of tick objects received from the server.
    """
    print("Ticks received:", ticks)
    raise NotImplementedError


def on_connect(ws: Any, response: str) -> None:
    """
    riggered when the WebSocket connection is established successfully.

    Parameters:
    ws (Any): The WebSocket object.
    response (str): Response received from the server on a successful connection.
    """

    logger.info("Connection established:", response)
    raise NotImplementedError


def on_close(ws: Any, code: int, reason: str) -> None:
    """
    Triggered when the WebSocket connection is closed.

    Parameters:
    ws (Any): The WebSocket object.
    code (int): WebSocket close event code.
    reason (str): The reason for closing the connection.
    """

    logger.error(f"""Connection closed: {code}, Reason: {reason}""")
    raise NotImplementedError


def on_error(ws: Any, code: int, reason: str) -> None:
    """
    Triggered when an error occurs during the WebSocket connection.

    Parameters:
    ws (Any): The WebSocket object.
    code (int): WebSocket error event code.
    reason (str): The reason for the error.
    """

    logger.error(f"Connection error: {code}, Reason: {reason}")
    raise NotImplementedError


def on_order_update(ws: Any, data: Dict[str, Any]) -> None:
    """
    Triggered when there is an order update for the connected user.

    Parameters:
    ws (Any): The WebSocket object.
    data (Dict[str, Any]): The order update data received from the server.
    """

    print("Order update received:", data)
    raise NotImplementedError


def on_noreconnect(ws: Any) -> None:
    """
    Triggered when the number of auto-reconnection attempts exceeds the allowed retries.

    Parameters:
    ws (Any): The WebSocket object.
    """

    logger.warning("No more reconnect attempts left.")
    raise NotImplementedError


def on_reconnect(ws: Any, attempts_count: int) -> None:
    """
    Triggered when an auto-reconnection is attempted.

    Parameters:
    ws (Any): The WebSocket object.
    attempts_count (int): The current reconnect attempt number.
    """

    logger.warning(f"Reconnect attempt {attempts_count}")
    raise NotImplementedError


def on_message(ws: Any, payload: bytes, is_binary: bool) -> None:
    """
    Triggered when a message is received from the server.

    Parameters:
    ws (Any): The WebSocket object.
    payload (bytes): The message payload received from the server.
    is_binary (bool): Whether the message is binary data or not.
    """

    logger.info("Message received (binary=%s): %s", is_binary, payload)
    raise NotImplementedError


def attach_handlers(kws: KiteTicker) -> KiteTicker:
    """
    Attach event handler functions to the Kite WebSocket object.

    Parameters:
    kws (KiteTicker): The KiteTicker WebSocket object to which event handlers will be attached.
    """
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_reconnect = on_reconnect
    kws.on_noreconnect = on_noreconnect
    kws.on_order_update = on_order_update
    kws.on_message = on_message

    return kws
