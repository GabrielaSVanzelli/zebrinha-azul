from dnc_project.utils.ingestion.trusted import ExporterWeather
import logging

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

logger = logging.getLogger(__name__)

DBNAME = "dnc_zebrinha_azul"
USER = "postgres"
PASSWORD = "password"
HOST = "postgres"
PORT = "5432"


@data_exporter
def export_data(data, *args, **kwargs):
    """
    """
    try:
        db_config = {
            "dbname": DBNAME,
            "user": USER,
            "password": PASSWORD,
            "host": HOST,
            "port": PORT
        }
        
        exporter = ExporterWeather(**db_config)
        exporter.create_table()

        exporter.insert_weather_data(data)
    except Exception as e:
        logger.error(e)


