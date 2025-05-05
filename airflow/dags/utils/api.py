import os
import logging
from typing import List, Dict, Optional, Any

# Import Socrata client library
from sodapy import Socrata

logger = logging.getLogger(__name__)

class SFFireIncidentsAPI:
    """
    Client to interact with the Socrata API for SFGov data, specifically
    tailored for fetching fire incidents but potentially reusable.

    Uses the sodapy library: https://pypi.org/project/sodapy/
    """
    def __init__(self, domain: str = "data.sfgov.org", app_token: Optional[str] = None, timeout: int = 60):
        """
        Initializes the Socrata client.

        Args:
            domain (str): The domain of the Socrata API (e.g., "data.sfgov.org").
            app_token (Optional[str]): An optional Socrata App Token. If None,
                                       it attempts to read from the SFGOV_APP_TOKEN
                                       environment variable. Defaults to None.
            timeout (int): The request timeout in seconds. Defaults to 60.

        Raises:
            RuntimeError: If the Socrata client cannot be initialized.
        """
        self.domain = domain
        _app_token = app_token or os.environ.get('SFGOV_APP_TOKEN')

        if _app_token:
            logger.info(f"Initializing Socrata client for {self.domain} with App Token.")
        else:
            # Use info level for consistency, warning might be too noisy if token is often absent
            logger.info(f"Initializing Socrata client for {self.domain} without App Token (requests may be throttled).")

        try:
            self._client = Socrata(self.domain, _app_token, timeout=timeout)
            logger.info(f"Socrata client initialized successfully for {self.domain}.")
        except Exception as e:
            logger.error(f"Failed to initialize Socrata client for {self.domain}: {e}", exc_info=True)
            raise RuntimeError(f"Could not initialize Socrata client: {e}") from e

    def get_data(self, dataset_id: str, **query_params: Any) -> List[Dict[str, Any]]:
        """
        Fetches all records for a specific Socrata dataset using provided filters.

        Args:
            dataset_id (str): The ID of the Socrata dataset (e.g., "wr8u-xric").
            **query_params: Arbitrary keyword arguments passed as filters/queries
                             to the Socrata API (e.g., where="call_type='Structure Fire'").

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the fetched records.

        Raises:
            Exceptions from the underlying sodapy library on failure (e.g., HTTPError, RequestException).
        """
        logger.info(f"Fetching data from dataset '{dataset_id}' with filters: {query_params}")
        results_generator = self._client.get_all(dataset_id, **query_params)
        results_list = list(results_generator) # Materialize the generator to fetch all data
        logger.info(f"Successfully fetched {len(results_list)} records from dataset '{dataset_id}'.")
        return results_list
