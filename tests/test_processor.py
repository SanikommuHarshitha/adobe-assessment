import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pytest
from processor import SearchKeywordProcessor


@pytest.fixture
def processor():
    """Create a processor instance for tests."""
    return SearchKeywordProcessor()


def test_extract_keyword_google(processor):
    """Test extracting keyword from Google referrer."""
    url = "http://www.google.com/search?q=ipod"

    domain, keyword = processor.extract_search_info(url)

    assert domain == "google.com"
    assert keyword == "ipod"


def test_extract_keyword_bing(processor):
    """Test extracting keyword from Bing referrer."""
    url = "http://www.bing.com/search?q=zune"

    domain, keyword = processor.extract_search_info(url)

    assert domain == "bing.com"
    assert keyword == "zune"


def test_extract_revenue(processor):
    """Test extracting revenue from product_list."""
    product_list = "Electronics;Ipod - Nano - 8GB;1;190;"

    revenue = processor.extract_revenue(product_list)

    assert revenue == 190.0


def test_is_purchase_event(processor):
    """Purchase event should return True when event 1 exists."""
    event_list = "1,12"

    assert processor.is_purchase(event_list) is True


def test_not_purchase_event(processor):
    """Non-purchase events should return False."""
    event_list = "2,12"

    assert processor.is_purchase(event_list) is False


def test_keyword_normalization(processor):
    """Keywords should be normalized to lowercase."""
    url = "http://www.google.com/search?q=Ipod"

    domain, keyword = processor.extract_search_info(url)

    assert keyword == "ipod"
