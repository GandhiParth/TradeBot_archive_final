import configparser
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime


def read_ini_file(file_location: str) -> Optional[configparser.ConfigParser]:
    """
    Reads an ini file and returns a ConfigParser object.

    Parameters:
    file_location (str): The path to the ini file.

    Returns:
    Optional[configparser.ConfigParser]: The ConfigParser object if the file exists, None otherwise.
    """

    if not Path(file_location).exists():
        return None

    config = configparser.ConfigParser()
    config.read(file_location)

    return config


def create_update_ini_file(file_location: str, section_key: str, data: Dict) -> None:
    """
    Creates or updates the config file at the file_location for the given section key
    and data. If the section exists, data will be added or updated.
    If the file doesn't exist, it will be created.

    Parameters:
    file_location (str): The path to the configuration file.
    section_key (str): The section key in the config file to add or update.
    data (Dict): The dictionary of data to add under the given section.
    """
    config = configparser.ConfigParser()

    config.read(file_location)

    if section_key not in config.sections():
        config[section_key] = {}

    for key, value in data.items():
        config[section_key][key] = value

    with open(file_location, "w") as configfile:
        config.write(configfile)


def get_today(format: str = "%Y-%m-%d") -> str:
    """
    Return today's date in the given format.

    Parameters:
    format (str): The format for the date string. Default is "%Y-%m-%d".

    Returns:
    str: Today's date formatted as specified.
    """
    today = datetime.now()
    formatted_date = today.strftime(format)
    return formatted_date
