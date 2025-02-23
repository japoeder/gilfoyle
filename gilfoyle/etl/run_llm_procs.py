"""
Run ETL processes.
"""
import logging

# from gilfoyle.etl.bachman_analysis import bachman_analysis
from gilfoyle.etl.modality.file_embeddings import file_embeddings
from gilfoyle.etl.modality.social_embeddings import social_embeddings
from gilfoyle.etl.modality.gentext_embeddings import gentext_embeddings


class RunLLMProcs:
    """
    Class driver for LLM processes.
    """

    def __init__(
        self,
        job_scope: str = None,
        load_year: int = None,
        source: str = None,
        doc_type: str = None,
        entity_type: str = None,
        live_load: bool = False,
        historical_load: bool = False,
        device: str = None,
    ):
        self.job_scope = job_scope
        self.load_year = load_year
        self.source = source
        self.doc_type = doc_type
        self.entity_type = entity_type
        self.live_load = live_load
        self.historical_load = historical_load
        self.device = device

        # Send to logging that we are starting the live news loader
        logging.info("Instantiating Bachman processor class...")

    def initiate_bachman_embeddings(self):
        """
        Initiate Bachman embeddings process.
        """

        response = None
        if self.source == "file":
            response = file_embeddings(
                source=self.source,
                doc_type=self.doc_type,
                entity_type=self.entity_type,
                device=self.device,
            )

        elif self.source == "social":
            response = social_embeddings(
                job_scope=self.job_scope,
                source=self.source,
                entity_type=self.entity_type,
                device=self.device,
            )

        elif self.source == "gentext":
            response = gentext_embeddings(
                job_scope=self.job_scope,
                source=self.source,
                entity_type=self.entity_type,
                device=self.device,
            )

        return response
