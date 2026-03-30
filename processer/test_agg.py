import pytest
from unittest.mock import MagicMock, patch
from agg import SpaceHit, HitProcesser, lambda_handler


# ── SpaceHit: parse_product_list ─────────────────────────────────────────────

class TestParseProductList:

    def test_parses_all_fields(self):
        hit = SpaceHit(**base_hit(product_list="Electronics;Laptop;2;999.99;event1;evar1"))
        hit.parse_product_list()
        assert hit.product_category == "Electronics"
        assert hit.product_name == "Laptop"
        assert hit.product_num_items == 2
        assert hit.product_revenue == 999.99
        assert hit.product_custom_events == "event1"
        assert hit.product_merch_evars == "evar1"

    def test_partial_product_list(self):
        hit = SpaceHit(**base_hit(product_list="Electronics;Laptop"))
        hit.parse_product_list()
        assert hit.product_category == "Electronics"
        assert hit.product_name == "Laptop"
        assert hit.product_num_items is None
        assert hit.product_revenue is None

    def test_empty_product_list(self):
        hit = SpaceHit(**base_hit(product_list=""))
        hit.parse_product_list()
        assert hit.product_revenue is None

    def test_missing_revenue_field(self):
        hit = SpaceHit(**base_hit(product_list="Electronics;Laptop;2;"))
        hit.parse_product_list()
        assert hit.product_revenue is None


# ── SpaceHit: parse_event_name ───────────────────────────────────────────────

class TestParseEventName:

    def test_purchase_event(self):
        hit = SpaceHit(**base_hit(event_list="1"))
        hit.parse_event_name()
        assert hit.event_name == "Purchase"

    def test_product_view_event(self):
        hit = SpaceHit(**base_hit(event_list="2"))
        hit.parse_event_name()
        assert hit.event_name == "Product view"

    def test_unknown_event(self):
        hit = SpaceHit(**base_hit(event_list="99"))
        hit.parse_event_name()
        assert hit.event_name is None

    def test_empty_event_list(self):
        hit = SpaceHit(**base_hit(event_list=""))
        hit.parse_event_name()
        assert hit.event_name is None


# ── SpaceHit: parse_referrer ─────────────────────────────────────────────────

class TestParseReferrer:

    def test_google_referrer(self):
        hit = SpaceHit(**base_hit(referrer="https://www.google.com/search?q=laptop+deals"))
        hit.parse_referrer()
        assert hit.engine_name == "google"
        assert hit.query_key == "laptop deals"

    def test_bing_referrer(self):
        hit = SpaceHit(**base_hit(referrer="https://www.bing.com/search?q=cheap+flights"))
        hit.parse_referrer()
        assert hit.engine_name == "bing"
        assert hit.query_key == "cheap flights"

    def test_yahoo_referrer(self):
        hit = SpaceHit(**base_hit(referrer="https://search.yahoo.com/search?p=shoes"))
        hit.parse_referrer()
        assert hit.engine_name == "yahoo"
        assert hit.query_key == "shoes"

    def test_non_search_engine_referrer(self):
        hit = SpaceHit(**base_hit(referrer="https://www.example.com/page"))
        hit.parse_referrer()
        assert hit.engine_name is None
        assert hit.query_key is None

    def test_empty_referrer(self):
        hit = SpaceHit(**base_hit(referrer=""))
        hit.parse_referrer()
        assert hit.engine_name is None

    def test_query_key_is_lowercased(self):
        hit = SpaceHit(**base_hit(referrer="https://www.google.com/search?q=LAPTOP"))
        hit.parse_referrer()
        assert hit.query_key == "laptop"


# ── HitProcesser: parse_hits ─────────────────────────────────────────────────

class TestParseHits:

    def test_parses_valid_rows(self):
        processor = make_processor()
        processor.parse_hits(sample_tsv())
        assert len(processor.hits) == 1
        assert processor.hits[0].ip == "192.168.1.1"

    def test_skips_invalid_rows(self):
        processor = make_processor()
        bad_tsv = "hit_time_gmt\tdate_time\n not_an_int\t2024-01-01"
        processor.parse_hits(bad_tsv)
        assert len(processor.hits) == 0

    def test_multiple_rows(self):
        processor = make_processor()
        tsv = sample_tsv(extra_rows=[
            "1700000002\t2024-01-01 00:00:02\tMozilla\t10.0.0.1\t2\tLA\tCA\tUS\tHome\thttp://site.com\t\t"
        ])
        processor.parse_hits(tsv)
        assert len(processor.hits) == 2


# ── HitProcesser: build_output ───────────────────────────────────────────────

class TestBuildOutput:

    def test_returns_expected_columns(self):
        processor = make_processor()
        processor.parse_hits(sample_tsv())
        df = processor.build_output()
        assert list(df.columns) == ["Search Engine Domain", "Search Keyword", "Revenue"]

    def test_filters_non_search_engine_hits(self):
        processor = make_processor()
        processor.parse_hits(sample_tsv())
        df = processor.build_output()
        assert all(df["Search Engine Domain"].isin(["google", "bing", "yahoo"]))

    def test_empty_purchase_list_does_not_crash(self):
        processor = make_processor()
        # Referrer from google but event_list=2 (not a purchase)
        processor.parse_hits(sample_tsv(event_list="2"))
        df = processor.build_output()
        assert df is not None

    def test_revenue_aggregated_by_engine_and_keyword(self):
        processor = make_processor()
        processor.parse_hits(sample_tsv(
            extra_rows=[
                "1700000002\t2024-01-01\tMozilla\t192.168.1.1\t1\tNY\tNY\tUS\tHome\thttp://site.com"
                "\t;Product;1;50.00;;\thttps://www.google.com/search?q=laptop"
            ]
        ))
        df = processor.build_output()
        google_row = df[df["Search Engine Domain"] == "google"]
        assert not google_row.empty


# ── lambda_handler ───────────────────────────────────────────────────────────

class TestLambdaHandler:

    def test_malformed_event_raises_value_error(self):
        with pytest.raises(ValueError, match="Malformed S3 event payload"):
            lambda_handler({}, None)

    @patch("lambda_function.HitProcesser")
    def test_valid_event_invokes_processor(self, mock_processer_cls, monkeypatch):
        monkeypatch.setenv("DESTINATION_BUCKET", "hits-file-agg-prod")

        mock_processor = MagicMock()
        mock_processor.build_output.return_value = MagicMock(__len__=lambda self: 5)
        mock_processer_cls.return_value = mock_processor

        lambda_handler(s3_event("hits-file-post-prod", "raw/test.tsv"), None)

        mock_processer_cls.assert_called_once_with("s3://hits-file-post-prod/raw/test.tsv")
        mock_processor.fetch_hits.assert_called_once()
        mock_processor.parse_hits.assert_called_once()
        mock_processor.build_output.assert_called_once()
        mock_processor.write_output.assert_called_once()


# ── Helpers ───────────────────────────────────────────────────────────────────

def base_hit(**overrides) -> dict:
    defaults = dict(
        hit_time_gmt=1700000000,
        date_time="2024-01-01 00:00:00",
        user_agent="Mozilla/5.0",
        ip="192.168.1.1",
        event_list="1",
        geo_city="New York",
        geo_region="NY",
        geo_country="US",
        pagename="Home",
        page_url="http://example.com",
        product_list=";Laptop;1;499.99;;",
        referrer="https://www.google.com/search?q=laptop",
    )
    return {**defaults, **overrides}


def make_processor() -> HitProcesser:
    with patch("boto3.client"):
        return HitProcesser("s3://hits-file-post-prod/raw/test.tsv")


def sample_tsv(event_list: str = "1", extra_rows: list[str] = None) -> str:
    header = "hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer"
    row = f"1700000001\t2024-01-01 00:00:01\tMozilla/5.0\t192.168.1.1\t{event_list}\tNew York\tNY\tUS\tHome\thttp://example.com\t;Laptop;1;499.99;;\thttps://www.google.com/search?q=laptop"
    lines = [header, row] + (extra_rows or [])
    return "\n".join(lines)


def s3_event(bucket: str, key: str) -> dict:
    return {
        "Records": [{
            "s3": {
                "bucket": {"name": bucket},
                "object": {"key": key},
            }
        }]
    }