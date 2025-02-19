"""
Run ETL processes.
"""
import logging

# from gilfoyle.etl.bachman_analysis import bachman_analysis
from gilfoyle.etl.bachman_embeddings import bachman_embeddings


class RunLLMProcs:
    """
    Class driver for LLM processes.
    """

    def __init__(
        self,
        job_scope: str = None,
        load_year: int = None,
        source: str = None,
        source_type: str = None,
        bachman_endpoint: str = None,
        mongo_db: str = None,
        live_load: bool = False,
        historical_load: bool = False,
    ):
        self.job_scope = job_scope
        self.load_year = load_year
        self.source = source
        self.source_type = source_type
        self.bachman_endpoint = bachman_endpoint
        self.mongo_db = mongo_db
        self.live_load = live_load
        self.historical_load = historical_load

        # Send to logging that we are starting the live news loader
        logging.info("Instantiating Bachman processor class...")

    def initiate_bachman_embeddings(self):
        """
        Initiate Bachman embeddings process.
        """
        logging.info("Running Bachman embeddings process...")
        response = bachman_embeddings(
            job_scope=self.job_scope,
            source=self.source,
            source_type=self.source_type,
            bachman_endpoint=self.bachman_endpoint,
            live_load=self.live_load,
            historical_load=self.historical_load,
            mongo_db=self.mongo_db,
        )

        return response
