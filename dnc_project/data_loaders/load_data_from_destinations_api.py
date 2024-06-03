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

API_KEY_TRAFFIC = os.getenv("API_KEY_TRAFFIC")
URL_TRAFFIC_API = os.getenv("URL_TRAFFIC_API")

logger = logging.getLogger(__name__)

api_client = APIClient(url_traffic_api=URL_TRAFFIC_API,
                        key_traffic_api=API_KEY_TRAFFIC)
data_parser = DataParser()

@data_loader
def load_data(*args, **kwargs) -> Optional[List[dict]]:
    """
    This functions calls destinations API and parse your response 
    """
    try:
        destinations = [(origin, destinations) for origin, destinations in zip(
            Variables().origin_city, Variables().destination_city)]

        all_parsed_data = []

        for i, (origin, destination) in enumerate(destinations):
            data = api_client.fetch_data_traffic(origin,
                                                destination)
            logging.info("Parsing data")
            parsed_data = data_parser.parse_traffic_data(data=data)
            all_parsed_data.append(eval(parsed_data))

        print(type(all_parsed_data[0]))
        return all_parsed_data
    except Exception as e:
        logger.error(f"An error occured: {e}")
        return None

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert len(output) != 50, 'The output is incompleted'
