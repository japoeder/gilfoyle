"""
Run ETL processes.
"""
import logging

from gilfoyle.etl.hendricks_hist_quote_loader import hendricks_hist_quote_loader
from gilfoyle.etl.hendricks_live_quote_loader import hendricks_live_quote_loader
from gilfoyle.etl.hendricks_hist_news_loader import hendricks_hist_news_loader
from gilfoyle.etl.hendricks_live_news_loader import hendricks_live_news_loader
from gilfoyle.etl.hendricks_hist_findata_loader import hendricks_hist_findata_loader
from gilfoyle.etl.hendricks_live_findata_loader import hendricks_live_findata_loader


class RunEtl:
    """
    Class driver for ETL processes.
    """

    def __init__(
        self,
        live_load: bool = False,
        historical_load: bool = False,
        job_scope: str = "full_ticker_set",
        sources: list = None,
        load_year: int = None,
        endpoints: dict = None,
        daily_flag: bool = None,
    ):
        self.live_load = live_load
        self.historical_load = historical_load
        self.job_scope = job_scope
        self.sources = sources
        self.load_year = load_year
        self.endpoints = endpoints
        self.daily_flag = daily_flag
        # Send to logging that we are starting the live news loader
        logging.info("Instantiating Hendricks ETL class...")

    def initiate_hendricks_quote_load(self):
        """
        Initiate the Hendricks quote loader.
        """
        print("Initiating Hendricks quote loader...")
        if self.live_load:
            hendricks_live_quote_loader(job_scope=self.job_scope)
        elif self.historical_load:
            hendricks_hist_quote_loader(
                job_scope=self.job_scope, load_year=self.load_year
            )

        return None

    def initiate_hendricks_news_load(self):
        """
        Initiate the Hendricks news loader.
        """
        print("Initiating Hendricks news loader...")

        successful_sources = []
        failed_sources = []

        for source in self.sources:
            source = [source]
            try:
                if self.live_load:
                    hendricks_live_news_loader(job_scope=self.job_scope, sources=source)
                elif self.historical_load:
                    hendricks_hist_news_loader(
                        job_scope=self.job_scope,
                        sources=source,
                        load_year=self.load_year,
                    )
                successful_sources.append(source)
            except:
                failed_sources.append(source)

        return {
            "successful_sources": successful_sources,
            "failed_sources": failed_sources,
        }

    def initiate_hendricks_fin_data_load(self):
        """
        Initiate the Hendricks financial data loader.
        """
        print("Initiating Hendricks financial data loader...")
        if self.live_load:
            hendricks_live_findata_loader(
                job_scope=self.job_scope,
                endpoints=self.endpoints,
                sources=self.sources,
                daily_flag=self.daily_flag,
            )
        elif self.historical_load:
            hendricks_hist_findata_loader(
                job_scope=self.job_scope,
                load_year=self.load_year,
                endpoints=self.endpoints,
                sources=self.sources,
                daily_flag=self.daily_flag,
            )

        return None
