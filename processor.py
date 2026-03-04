"""
Search Keyword Performance Processor
Analyzes hit-level data to determine revenue generated from external search engines.
"""

import csv
import io
from collections import defaultdict
from urllib.parse import urlparse, parse_qs
from datetime import datetime


# The site being analyzed - referrers from this domain are internal (not search engines)
OWNED_DOMAIN = "esshopzilla.com"

# Common search query parameter names used across search engines
SEARCH_QUERY_PARAMS = ["q", "p", "query", "search", "qs", "text", "keyword"]

# Per Appendix A: event 1 = Purchase. Revenue is only actualized when this event is present.
PURCHASE_EVENT = "1"


class SearchKeywordProcessor:
    """
    Processes hit-level web analytics data to compute revenue
    attributed to external search engine keywords.
    """

    def __init__(self):
        self.revenue_by_keyword = defaultdict(float)  # (domain, keyword) -> revenue

    def is_search_engine_referrer(self, referrer: str) -> bool:
        """
        Check if the referrer is an external search engine.
        A referrer is considered a search engine if:
        1. It is external (not from our own domain)
        2. It contains a recognizable search query parameter
        """
        if not referrer:
            return False
        try:
            parsed = urlparse(referrer)
            netloc = parsed.netloc.lower()
            # Must be external
            if OWNED_DOMAIN in netloc:
                return False
            # Must have a search query param
            query_params = parse_qs(parsed.query)
            return any(param in query_params for param in SEARCH_QUERY_PARAMS)
        except Exception:
            return False

    def extract_search_info(self, referrer: str) -> tuple[str, str] | tuple[None, None]:
        """
        Dynamically extract the search engine domain and keyword from a referrer URL.
        Does not rely on a hardcoded list of search engines.
        Returns (domain, keyword) or (None, None) if not a search engine referrer.
        """
        if not referrer:
            return None, None
        try:
            parsed = urlparse(referrer)
            netloc = parsed.netloc.lower()

            # Must be external
            if OWNED_DOMAIN in netloc:
                return None, None

            query_params = parse_qs(parsed.query)

            # Try each common search param in priority order
            for param in SEARCH_QUERY_PARAMS:
                if param in query_params:
                    keyword = query_params[param][0].strip().lower()
                    if keyword:
                        # Strip www. for a clean domain name
                        domain = netloc.replace("www.", "", 1)
                        return domain, keyword

            return None, None
        except Exception:
            return None, None

    def is_purchase(self, event_list: str) -> bool:
        """
        Check if the hit contains a purchase event.
        Per Appendix A of the requirements: event 1 = Purchase.
        Revenue is only actualized when the purchase event is set in the event_list.
        """
        if not event_list:
            return False
        return PURCHASE_EVENT in [e.strip() for e in event_list.split(",")]

    def extract_revenue(self, product_list: str) -> float:
        """
        Extract total revenue from product_list.
        Format: category;product;quantity;price;...
        Revenue is the 4th field (index 3).
        """
        if not product_list:
            return 0.0
        total = 0.0
        products = product_list.split(",")
        for product in products:
            fields = product.split(";")
            if len(fields) >= 4 and fields[3].strip():
                try:
                    total += float(fields[3].strip())
                except ValueError:
                    pass
        return total

    def process_file(self, file_content: str) -> dict:
        """
        Process the tab-separated hit-level data file content.

        Strategy: Track the most recent external search engine referrer per visitor
        session (identified by IP + user_agent). Per Appendix B, revenue is only
        actualized when the purchase event (event 1) is present. When a purchase
        occurs, attribute revenue to that session's search referrer.

        Returns a dict of (domain, keyword) -> revenue.
        """
        self.revenue_by_keyword = defaultdict(float)

        # session_key -> (domain, keyword) of most recent search engine referrer
        session_search: dict = {}

        reader = csv.DictReader(io.StringIO(file_content), delimiter="\t")

        for row in reader:
            referrer = row.get("referrer", "").strip()
            event_list = row.get("event_list", "").strip()
            product_list = row.get("product_list", "").strip()
            ip = row.get("ip", "").strip()
            user_agent = row.get("user_agent", "").strip()

            session_key = (ip, user_agent)

            # Track the most recent search engine referrer for this session
            domain, keyword = self.extract_search_info(referrer)
            if domain and keyword:
                session_search[session_key] = (domain, keyword)

            # Per Appendix B: revenue is only actualized when the purchase event is set
            if self.is_purchase(event_list):
                revenue = self.extract_revenue(product_list)
                if revenue > 0:
                    search_info = session_search.get(session_key)
                    if search_info:
                        self.revenue_by_keyword[search_info] += revenue

        return dict(self.revenue_by_keyword)

    def generate_output(self, revenue_data: dict, date_str: str) -> tuple[str, str]:
        """
        Generate the output tab-delimited file content sorted by revenue descending.
        Returns (filename, file_content).
        """
        filename = f"{date_str}_SearchKeywordPerformance.tab"

        lines = ["Search Engine Domain\tSearch Keyword\tRevenue"]
        sorted_data = sorted(revenue_data.items(), key=lambda x: x[1], reverse=True)

        for (domain, keyword), revenue in sorted_data:
            lines.append(f"{domain}\t{keyword}\t{revenue:.2f}")

        return filename, "\n".join(lines)

    def extract_date_from_content(self, file_content: str) -> str:
        """Extract the date from the first data row's date_time column."""
        try:
            reader = csv.DictReader(io.StringIO(file_content), delimiter="\t")
            for row in reader:
                dt_str = row.get("date_time", "").strip()
                if dt_str:
                    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                    return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
        return datetime.now().strftime("%Y-%m-%d")
