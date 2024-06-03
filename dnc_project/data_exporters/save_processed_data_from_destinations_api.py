from dnc_project.utils.ingestion.trusted import Exporter
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
        exporter = Exporter(
            dbname=DBNAME,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        logger.info("Save data on postgres Table")
        exporter.main(data)
        logger.info("Saved data on postgres Table")
    except Exception as e:
        logger.error(e)


