"""
Unit tests for SearchKeywordProcessor
"""

import pytest
from src.processor import SearchKeywordProcessor


@pytest.fixture
def processor():
    return SearchKeywordProcessor()


# ---------------------------------------------------------------------------
# is_search_engine_referrer
# ---------------------------------------------------------------------------

def test_google_referrer_detected(processor):
    assert processor.is_search_engine_referrer("http://www.google.com/search?q=ipod") is True

def test_bing_referrer_detected(processor):
    assert processor.is_search_engine_referrer("http://www.bing.com/search?q=zune") is True

def test_yahoo_referrer_detected(processor):
    assert processor.is_search_engine_referrer("http://search.yahoo.com/search?p=cd+player") is True

def test_unknown_search_engine_detected(processor):
    assert processor.is_search_engine_referrer("https://duckduckgo.com/?q=headphones") is True

def test_internal_referrer_not_detected(processor):
    assert processor.is_search_engine_referrer("http://www.esshopzilla.com/cart/") is False

def test_empty_referrer_not_detected(processor):
    assert processor.is_search_engine_referrer("") is False

def test_none_referrer_not_detected(processor):
    assert processor.is_search_engine_referrer(None) is False


# ---------------------------------------------------------------------------
# extract_search_info
# ---------------------------------------------------------------------------

def test_google_domain_and_keyword(processor):
    domain, keyword = processor.extract_search_info("http://www.google.com/search?hl=en&q=Ipod&aq=f")
    assert domain == "google.com"
    assert keyword == "ipod"

def test_bing_domain_and_keyword(processor):
    domain, keyword = processor.extract_search_info("http://www.bing.com/search?q=Zune&form=QBLH")
    assert domain == "bing.com"
    assert keyword == "zune"

def test_yahoo_domain_and_keyword(processor):
    domain, keyword = processor.extract_search_info("http://search.yahoo.com/search?p=cd+player&ei=UTF-8")
    assert domain == "search.yahoo.com"
    assert keyword == "cd player"

def test_internal_url_returns_none(processor):
    domain, keyword = processor.extract_search_info("http://www.esshopzilla.com/cart/")
    assert domain is None
    assert keyword is None

def test_empty_referrer_returns_none(processor):
    domain, keyword = processor.extract_search_info("")
    assert domain is None
    assert keyword is None


# ---------------------------------------------------------------------------
# is_purchase
# ---------------------------------------------------------------------------

def test_purchase_event_alone(processor):
    assert processor.is_purchase("1") is True

def test_purchase_among_multiple_events(processor):
    assert processor.is_purchase("2,1,12") is True

def test_non_purchase_event(processor):
    assert processor.is_purchase("2") is False

def test_shopping_cart_event_not_purchase(processor):
    assert processor.is_purchase("12") is False

def test_empty_event_list_not_purchase(processor):
    assert processor.is_purchase("") is False


# ---------------------------------------------------------------------------
# extract_revenue
# ---------------------------------------------------------------------------

def test_single_product_revenue(processor):
    assert processor.extract_revenue("Electronics;Zune - 32GB;1;250;") == 250.0

def test_multiple_products_revenue_summed(processor):
    assert processor.extract_revenue("Electronics;Zune;1;250;,Electronics;Ipod;1;100;") == 350.0

def test_empty_price_field_returns_zero(processor):
    assert processor.extract_revenue("Electronics;Zune - 32GB;1;;") == 0.0

def test_empty_product_list_returns_zero(processor):
    assert processor.extract_revenue("") == 0.0


# ---------------------------------------------------------------------------
# process_file - integration tests
# Purchase rows always have an internal referrer (checkout page).
# The search referrer appears earlier in the session on a different row.
# ---------------------------------------------------------------------------

SAMPLE_DATA = (
    "hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer\n"
    # Session 1: arrives from Bing searching Zune, then purchases
    "1000\t2009-09-27 06:00:00\tMozilla/5.0\t10.0.0.1\t2\tRochester\tNY\tUS\tZune Page\thttp://www.esshopzilla.com/product/1\tElectronics;Zune;1;;\thttp://www.bing.com/search?q=Zune\n"
    "1001\t2009-09-27 06:01:00\tMozilla/5.0\t10.0.0.1\t1\tRochester\tNY\tUS\tOrder Complete\thttps://www.esshopzilla.com/checkout/?a=complete\tElectronics;Zune;1;250;\thttps://www.esshopzilla.com/checkout/?a=confirm\n"
    # Session 2: arrives from Google searching Ipod, then purchases
    "1002\t2009-09-27 06:02:00\tMozilla/5.0\t10.0.0.2\t2\tDuncan\tOK\tUS\tIpod Page\thttp://www.esshopzilla.com/product/2\tElectronics;Ipod;1;;\thttp://www.google.com/search?q=Ipod\n"
    "1003\t2009-09-27 06:03:00\tMozilla/5.0\t10.0.0.2\t1\tDuncan\tOK\tUS\tOrder Complete\thttps://www.esshopzilla.com/checkout/?a=complete\tElectronics;Ipod;1;190;\thttps://www.esshopzilla.com/checkout/?a=confirm\n"
    # Session 3: arrives from Yahoo, views product but does NOT purchase
    "1004\t2009-09-27 06:04:00\tMozilla/5.0\t10.0.0.3\t2\tSalem\tOR\tUS\tSome Page\thttp://www.esshopzilla.com/product/3\tElectronics;CD Player;1;;\thttp://search.yahoo.com/search?p=cd+player\n"
)

def test_revenue_attributed_to_search_referrer(processor):
    result = processor.process_file(SAMPLE_DATA)
    assert ("bing.com", "zune") in result
    assert result[("bing.com", "zune")] == 250.0
    assert ("google.com", "ipod") in result
    assert result[("google.com", "ipod")] == 190.0

def test_non_purchase_sessions_excluded(processor):
    result = processor.process_file(SAMPLE_DATA)
    assert ("search.yahoo.com", "cd player") not in result

def test_only_purchase_rows_generate_revenue(processor):
    result = processor.process_file(SAMPLE_DATA)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# generate_output
# ---------------------------------------------------------------------------

def test_output_sorted_by_revenue_descending(processor):
    revenue_data = {
        ("google.com", "ipod"): 190.0,
        ("bing.com", "zune"): 250.0,
    }
    filename, content = processor.generate_output(revenue_data, "2009-09-27")
    lines = content.strip().split("\n")
    assert lines[0] == "Search Engine Domain\tSearch Keyword\tRevenue"
    assert "bing.com" in lines[1]
    assert "google.com" in lines[2]

def test_output_filename_format(processor):
    filename, _ = processor.generate_output({("google.com", "ipod"): 190.0}, "2009-09-27")
    assert filename == "2009-09-27_SearchKeywordPerformance.tab"

def test_output_has_correct_columns(processor):
    _, content = processor.generate_output({("google.com", "ipod"): 190.0}, "2009-09-27")
    lines = content.strip().split("\n")
    assert lines[1] == "google.com\tipod\t190.00"


# ---------------------------------------------------------------------------
# extract_date_from_content
# ---------------------------------------------------------------------------

def test_date_extracted_from_data(processor):
    date = processor.extract_date_from_content(SAMPLE_DATA)
    assert date == "2009-09-27"
