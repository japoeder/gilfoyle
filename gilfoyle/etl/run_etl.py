"""
Run ETL processes.
"""

from gilfoyle.etl.hendricks_hist_loader import hendricks_hist_loader
from gilfoyle.etl.hendricks_live_loader import hendricks_live_loader


class RunEtl:
    """
    Class driver for ETL processes.
    """

    def __init__(self, live_load: bool = False, historical_load: bool = False):
        self.live_load = live_load
        self.historical_load = historical_load

    def initiate_hendricks_load(self):
        """
        Initiate the Hendricks quote loader.
        """
        print("Initiating Hendricks quote loader...")
        if self.live_load:
            hendricks_live_loader()
        elif self.historical_load:
            hendricks_hist_loader()

        return None
