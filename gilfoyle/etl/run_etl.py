"""
Run ETL processes.
"""

from gilfoyle.etl.hendricks_hist_quote_loader import hendricks_hist_quote_loader
from gilfoyle.etl.hendricks_live_quote_loader import hendricks_live_quote_loader
from gilfoyle.etl.hendricks_hist_news_loader import hendricks_hist_news_loader
from gilfoyle.etl.hendricks_live_news_loader import hendricks_live_news_loader


class RunEtl:
    """
    Class driver for ETL processes.
    """

    def __init__(
        self,
        live_load: bool = False,
        historical_load: bool = False,
        job_scope: str = "comp_load",
        sources: str = None,
    ):
        self.live_load = live_load
        self.historical_load = historical_load
        self.job_scope = job_scope
        self.sources = sources

    def initiate_hendricks_quote_load(self):
        """
        Initiate the Hendricks quote loader.
        """
        print("Initiating Hendricks quote loader...")
        if self.live_load:
            hendricks_live_quote_loader(job_scope=self.job_scope)
        elif self.historical_load:
            hendricks_hist_quote_loader(job_scope=self.job_scope)

        return None

    def initiate_hendricks_news_load(self):
        """
        Initiate the Hendricks news loader.
        """
        print("Initiating Hendricks news loader...")
        if self.live_load:
            hendricks_live_news_loader(job_scope=self.job_scope, sources=self.sources)
        elif self.historical_load:
            hendricks_hist_news_loader(job_scope=self.job_scope, sources=self.sources)

        return None
