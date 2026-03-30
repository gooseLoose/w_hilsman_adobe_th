import csv
import pandas as pd
import logging
import boto3
import io

from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

from urllib.parse import urlparse, parse_qs

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """AWS Lambda entry point — S3 trigger passes bucket/key in the event."""
    s3_uri = None
    try:
        record = event["Records"][0]["s3"]
        bucket = record["bucket"]["name"]
        key    = record["object"]["key"]
        s3_uri = f"s3://{bucket}/{key}"
        logger.info("Processing %s", s3_uri)
 
        processor = HitProcesser(s3_uri)
        processor.parse_hits(processor.fetch_hits())
        output_df = processor.build_output()
        processor.write_output(output_df)
        logger.info("Successfully wrote %d rows to output", len(output_df))
 
    except (KeyError, IndexError) as e:
        raise ValueError(f"Malformed S3 event payload: {e}") from e
    except Exception as e:
        logger.error("Processing failed for %s: %s", s3_uri, e)
        raise

@dataclass
class SpaceHit:
    """Hit feed data from space bloom."""
    hit_time_gmt: int
    date_time: str
    user_agent: str
    ip: str
    event_list: str
    geo_city: str
    geo_region: str
    geo_country: str
    pagename: str
    page_url: str
    product_list: str
    referrer: str

    # Event String Field
    event_name: Optional[str] = field(default=None)

    # Product Fields
    product_category: Optional[str] = field(default=None)
    product_name: Optional[str] = field(default=None)
    product_num_items: Optional[int] = field(default=None)
    product_revenue: Optional[float] = field(default=None)
    product_custom_events: Optional[str] = field(default=None)
    product_merch_evars: Optional[str] = field(default=None)

    # Parsed Ref and Keyword
    engine_name: Optional[str] = field(default=None)
    query_key: Optional[str] = field(default=None)


    def parse_product_list(self):
        "Parse data from product list. Data provdied has singular product list so assuming singular product here."
        if not self.product_list:
            return
        parts = self.product_list.split(";")
        self.product_category    = parts[0] if len(parts) > 0 else None
        self.product_name        = parts[1] if len(parts) > 1 else None
        self.product_num_items   = int(parts[2]) if len(parts) > 2 and parts[2] else None
        self.product_revenue     = float(parts[3]) if len(parts) > 3 and parts[3] else None
        self.product_custom_events = parts[4] if len(parts) > 4 else None
        self.product_merch_evars   = parts[5] if len(parts) > 5 else None

    def parse_event_name(self):
        "Create event name column using LUT provided in PDF sheet"
        ev_lot = { "1":  "Purchase",
                   "2":  "Product view",
                   "10": "Shopping Cart Open",
                   "11": "Shopping Cart Checkout",
                   "12": "Shopping Cart Add",
                   "13": "Shopping Cart Remove",
                   "14": "Shopping Cart View",
        }
        self.event_name = ev_lot.get(self.event_list)

    def parse_referrer(self):
        "Use ULRLib to parse out host name and query params to identify keywords"
        if not self.referrer:
            return
        
        ref_map = {
            "google": ["google", "q"],
            "bing": ["bing", "q"],
            "yahoo": ["yahoo", "p"],
        }
        
        pr = urlparse(self.referrer)
        host = pr.hostname or ""
        parameters = parse_qs(pr.query)

        for engine, q_map in ref_map.items():
            if engine in host:
                self.engine_name = engine
                self.query_key = parameters.get(q_map[1], [None])[0].lower()
                return

    def to_dict(self) -> dict:
        return {
            "hit_time_gmt":         self.hit_time_gmt,
            "date_time":            self.date_time,
            "user_agent":           self.user_agent,
            "ip":                   self.ip,
            "event_list":           self.event_list,
            "geo_city":             self.geo_city,
            "geo_region":           self.geo_region,
            "geo_country":          self.geo_country,
            "pagename":             self.pagename,
            "page_url":             self.page_url,
            "product_list_raw":     self.product_list,
            "referrer":             self.referrer,
            "event_name":           self.event_name,
            "product_category":     self.product_category,
            "product_name":         self.product_name,
            "product_num_items":    self.product_num_items,
            "product_revenue":       self.product_revenue,
            "product_custom_events": self.product_custom_events,
            "product_merch_evars":   self.product_merch_evars,
            "engine_name":           self.engine_name,
            "query_key":             self.query_key
        }

class HitProcesser:
    """Reads a tab delimted file from S3 and parses said file into a final organized output"""

    def __init__(self, s3_uri: str):
        self.s3_uri = s3_uri
        self.bucket, self.key = self._parse_s3_uri(s3_uri)
        self.s3_client = boto3.client("s3")
        self.hits: list[SpaceHit] = []


    def fetch_hits(self):
        "Fetch hits from an S3 object"
        response = self.s3_client.get_object(Bucket=self.bucket, Key=self.key)
        return response["Body"].read().decode("utf-8")


    def parse_hits(self, data):
        """Parse file into SpaceHit objects for data validation and transofrmation"""
        reader = csv.DictReader(data.splitlines(), delimiter="\t")
        for i, row in enumerate(reader, start=1):
            try:
                hit = SpaceHit(
                    hit_time_gmt=int(row["hit_time_gmt"]),
                    date_time=row["date_time"],
                    user_agent=row["user_agent"],
                    ip=row["ip"],
                    event_list=row["event_list"],
                    geo_city=row["geo_city"],
                    geo_region=row["geo_region"],
                    geo_country=row["geo_country"],
                    pagename=row["pagename"],
                    page_url=row["page_url"],
                    product_list=row["product_list"],
                    referrer=row["referrer"],
                )
                hit.parse_product_list()
                hit.parse_event_name()
                hit.parse_referrer()
                self.hits.append(hit)
            except (KeyError, ValueError) as e:
                print(f"  [warn] row {i} skipped — {e}")


    def write_output(self, grouped_df: pd.DataFrame):
        buffer = io.StringIO()
        grouped_df.to_csv(buffer, sep="\t", index=False)

        s3_key = self._build_s3_output_key()
        self.s3_client.put_object(
            Bucket="hits-file-agg-prod",
            Key=s3_key,
            Body=buffer.getvalue().encode("utf-8"),
            ContentType="text/plain",
        )


    def build_output(self) -> pd.DataFrame:
        """Filter out for simply values in valid search engines. Build output."""
        se_engines = {"google", "bing", "yahoo"}
        filter_hits = [val for val in self.hits if val.engine_name in se_engines]
        filter_purchase = [val for val in self.hits if val.event_name == 'Purchase']

        # Build first dataframe to identify engine, ip and query key
        base_df = (
            pd.DataFrame(val.to_dict() for val in filter_hits)[["engine_name", "ip" ,"query_key", "product_revenue"]]
        )
        rev_df = (
            pd.DataFrame(val.to_dict() for val in filter_purchase)[["ip", "product_revenue"]]
            .groupby(["ip"], as_index=False)["product_revenue"]
            .sum()
        )

        # Join dataframes based on IP to determine revenue
        df = base_df.merge(rev_df, on="ip", how="left", suffixes=("", "_updated"))
        df["product_revenue"] = df["product_revenue_updated"].combine_first(df["product_revenue"])
        df = df.drop(columns=["product_revenue_updated"])

        # Build out Final Dataframe to return based on client requirements
        final_df = (
            df.drop(columns=["ip"])
            .groupby(["engine_name", "query_key"], as_index=False)["product_revenue"]
            .sum()
            .sort_values("product_revenue", ascending=False)
            .rename(columns={
                "engine_name":     "Search Engine Domain",
                "query_key":       "Search Keyword",
                "product_revenue": "Revenue",
                }
            ))
        return final_df
    
    def _parse_s3_uri(self, uri: str) -> tuple[str, str]:
            """Split s3://bucket/key/path into (bucket, key)."""
            if not uri.startswith("s3://"):
                raise ValueError(f"Expected an s3:// URI, got: {uri}")
            parts = uri[5:].split("/", 1)
            return parts[0], parts[1]
    
    def _build_filename(self) -> str:
        today_format = datetime.today().strftime('%Y-%m-%d')
        return f"{today_format}_SearchKeywordPerformance.tab"
    
    def _build_s3_output_key(self) -> str:
        today = datetime.today().strftime('%Y-%m-%d')
        filename = self._build_filename()
        return f"{today}/{filename}"
