from dnc_project.utils.ingestion.raw import (
    APIClient,
    DataParser
    )
from dnc_project.utils.variables import Variables
from typing import List, Optional, Any
from dotenv import load_dotenv
import os
import logging

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

load_dotenv()

URL_WEATHER_API = os.getenv("URL_WEATHER_API")
API_KEY_WEATHER = os.getenv("API_KEY_WEATHER")

@data_loader
def load_data(*args, **kwargs):
    """
        This functions calls Weather API and parse your response 
    """
    try:

        all_parsed_data = []

        for city in Variables().all_cities:
            data = api_client.fetch_data_weather(city=city)
            logging.info("Parsing data")
            parsed_data = data_parser.parse_weather_data(data=data)
            all_parsed_data.append(eval(parsed_data))

        print(all_parsed_data)
        return all_parsed_data
    except Exception as e:
        logging.error(f"An error occured: {e}")
        return None


# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert len(output) != 100, 'The output is incompleted'
