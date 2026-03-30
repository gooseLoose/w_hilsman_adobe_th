from dataclasses import dataclass, field
from typing import Optional

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

    def event_loookup(self):
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


    def to_dict(self) -> dict:
        return {
            "hit_time_gmt":        self.hit_time_gmt,
            "date_time":           self.date_time,
            "user_agent":          self.user_agent,
            "ip":                  self.ip,
            "event_list":          self.event_list,
            "geo_city":            self.geo_city,
            "geo_region":          self.geo_region,
            "geo_country":         self.geo_country,
            "pagename":            self.pagename,
            "page_url":            self.page_url,
            "product_list_raw":    self.product_list,
            "referrer":            self.referrer,
            "event_name":          self.event_name,
            "product_category":    self.product_category,
            "product_name":        self.product_name,
            "product_num_items":   self.product_num_items,
            "product_revenue":     self.product_revenue,
            "product_custom_events": self.product_custom_events,
            "product_merch_evars": self.product_merch_evars,
        }
