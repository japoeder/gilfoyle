"""
Run ETL processes.
"""
import logging
from gilfoyle.etl.hendricks_ingestion import hendricks_ingestion


class RunIngestion:
    """
    Class driver for ETL processes.
    """

    def __init__(
        self,
        job_scope: str = None,
        sources: list = None,
        load_year: int = None,
        target_endpoints: dict = None,
        daily_fmp_flag: bool = None,
        hendricks_endpoint: str = None,
        live_load: bool = False,
        historical_load: bool = False,
        mongo_db: str = None,
        reddit_load: bool = False,
        subreddits: list = None,
        keywords: list = None,
    ):
        self.job_scope = job_scope
        self.sources = sources
        self.load_year = load_year
        self.target_endpoints = target_endpoints
        self.daily_fmp_flag = daily_fmp_flag
        self.hendricks_endpoint = hendricks_endpoint
        self.live_load = live_load
        self.historical_load = historical_load
        self.mongo_db = mongo_db
        self.reddit_load = reddit_load
        self.subreddits = subreddits
        self.keywords = keywords
        # Send to logging that we are starting the live news loader
        logging.info("Instantiating Hendricks loading class...")

    def initiate_hendricks_ingestion(self):
        """
        Initiate Hendricks loading process.
        """
        logging.info("Running Hendricks loading process...")
        response = hendricks_ingestion(
            job_scope=self.job_scope,
            sources=self.sources,
            load_year=self.load_year,
            target_endpoints=self.target_endpoints,
            daily_fmp_flag=self.daily_fmp_flag,
            hendricks_endpoint=self.hendricks_endpoint,
            live_load=self.live_load,
            historical_load=self.historical_load,
            mongo_db=self.mongo_db,
            reddit_load=self.reddit_load,
            subreddits=self.subreddits,
            keywords=self.keywords,
        )

        return response
