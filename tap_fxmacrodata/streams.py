"""The streams FXMacroData publishes."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_fxmacrodata.client import PAGE_SIZE, FXMacroDataStream


class AnnouncementsStream(FXMacroDataStream):
    """Macroeconomic observations, one record per release of one indicator.

    ``announcement_datetime`` is the instant the figure was published, which is
    what makes the series usable point-in-time: a model can filter on it rather
    than assume a figure was known on its reference date.
    """

    name = "announcements"
    path = "/announcements/{currency}/{indicator}"
    primary_keys = ("announcement_id",)
    replication_key = "date"
    is_sorted = False

    schema = th.PropertiesList(
        th.Property("announcement_id", th.StringType, required=True),
        th.Property("currency", th.StringType, required=True),
        th.Property("indicator", th.StringType, required=True),
        th.Property("date", th.DateType, description="Reference date of the observation."),
        th.Property("val", th.NumberType, description="Published value; null when not reported."),
        th.Property(
            "announcement_datetime",
            th.IntegerType,
            description="Instant the figure was published, epoch seconds.",
        ),
        th.Property("announcement_datetime_local", th.StringType),
        th.Property("previous_value", th.NumberType),
        th.Property("change_from_previous", th.NumberType),
        th.Property("pct_change_from_previous", th.NumberType),
        th.Property("previous_announcement_datetime", th.IntegerType),
        th.Property("observation_id", th.StringType),
        th.Property("source", th.StringType),
        th.Property("source_url", th.StringType),
        th.Property("source_url_scope", th.StringType),
        th.Property("capture_time_basis", th.StringType),
        th.Property("publication_time_status", th.StringType),
        th.Property("collected_at_iso", th.DateTimeType),
        th.Property("collected_at_ns_string", th.StringType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict] | None:
        """One partition per currency/indicator pair, so state is kept per series."""
        indicators = self.config.get("indicators") or []
        return [
            {"currency": currency.upper(), "indicator": indicator}
            for currency in self.config.get("currencies", ["USD"])
            for indicator in indicators
        ] or None

    def get_url_params(self, context: dict | None, next_page_token: t.Any) -> dict:  # noqa: ANN401
        params: dict[str, t.Any] = {"limit": PAGE_SIZE}
        start = self._start_date(context)
        if start:
            params["start_date"] = start
        return params

    def _start_date(self, context: dict | None) -> str | None:
        """Resolve the incremental cursor as a plain date.

        The replication key is a reference *date*, not a timestamp, so the
        bookmark is read raw rather than through ``get_starting_timestamp``,
        which requires a timestamp-typed key and would force this column to be
        stored as one.
        """
        bookmark = self.get_starting_replication_key_value(context)
        value = bookmark or self.config.get("start_date")
        return str(value)[:10] if value else None

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        """Stamp the series identity onto each row.

        Currency and indicator travel in the path rather than in the record, so
        without this a table holding several series could not be told apart.
        """
        context = context or {}
        row["currency"] = context.get("currency")
        row["indicator"] = context.get("indicator")
        return row


class ReleaseCalendarStream(FXMacroDataStream):
    """Scheduled and recent releases, for planning rather than backfilling."""

    name = "release_calendar"
    path = "/calendar/{currency}"
    primary_keys = ("calendar_event_id",)

    schema = th.PropertiesList(
        th.Property("calendar_event_id", th.StringType, required=True),
        th.Property("currency", th.StringType, required=True),
        th.Property("release", th.StringType),
        th.Property("name", th.StringType),
        th.Property("date", th.DateType),
        th.Property("announcement_datetime", th.IntegerType),
        th.Property("announcement_datetime_utc", th.DateTimeType),
        th.Property("announcement_datetime_local", th.StringType),
        th.Property("release_date_confirmed", th.BooleanType),
        th.Property("event_importance", th.StringType),
        th.Property("market_tier", th.StringType),
        th.Property("top_tier_for_currency", th.BooleanType),
        th.Property("source", th.StringType),
        th.Property("source_url", th.StringType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict] | None:
        return [
            {"currency": currency.upper()}
            for currency in self.config.get("currencies", ["USD"])
        ] or None

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row["currency"] = (context or {}).get("currency")
        return row


class DataCatalogueStream(FXMacroDataStream):
    """Which indicators a currency publishes.

    Useful on its own, and the way to discover what to list in ``indicators``.
    """

    name = "data_catalogue"
    path = "/data_catalogue/{currency}"
    primary_keys = ("currency", "indicator")

    schema = th.PropertiesList(
        th.Property("currency", th.StringType, required=True),
        th.Property("indicator", th.StringType, required=True),
        th.Property("source", th.StringType),
        th.Property("source_url", th.StringType),
        th.Property("unit", th.StringType),
        th.Property("frequency", th.StringType),
        th.Property("latest_available_date", th.StringType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict] | None:
        return [
            {"currency": currency.upper()}
            for currency in self.config.get("currencies", ["USD"])
        ] or None

    def parse_response(self, response) -> t.Iterable[dict]:  # noqa: ANN001
        """The catalogue is an object keyed by indicator, not a data array."""
        if response.status_code in (401, 403, 404):
            return
        payload = response.json()
        if not isinstance(payload, dict):
            return
        for indicator, meta in payload.items():
            if isinstance(meta, dict):
                yield {"indicator": indicator, **meta}

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        row["currency"] = (context or {}).get("currency")
        return {key: row.get(key) for key in self.schema["properties"]}


class ForexRatesStream(FXMacroDataStream):
    """Official reference rates for a currency pair.

    A pair resolves whenever both currencies are covered: the API stores each
    pair in one direction and derives the inverse or the cross itself, so
    ``EUR/JPY`` works even though only the USD legs are stored.
    """

    name = "forex_rates"
    path = "/forex/{base}/{quote}"
    primary_keys = ("base", "quote", "date")
    replication_key = "date"
    is_sorted = False

    schema = th.PropertiesList(
        th.Property("base", th.StringType, required=True),
        th.Property("quote", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("val", th.NumberType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict] | None:
        pairs = []
        for pair in self.config.get("fx_pairs") or []:
            if "/" in pair:
                base, quote = pair.split("/", 1)
                pairs.append({"base": base.lower(), "quote": quote.lower()})
        return pairs or None

    def get_url_params(self, context: dict | None, next_page_token: t.Any) -> dict:  # noqa: ANN401
        params: dict[str, t.Any] = {"limit": PAGE_SIZE}
        start = self._start_date(context)
        if start:
            params["start_date"] = start
        return params

    def _start_date(self, context: dict | None) -> str | None:
        """Resolve the incremental cursor as a plain date.

        The replication key is a reference *date*, not a timestamp, so the
        bookmark is read raw rather than through ``get_starting_timestamp``,
        which requires a timestamp-typed key and would force this column to be
        stored as one.
        """
        bookmark = self.get_starting_replication_key_value(context)
        value = bookmark or self.config.get("start_date")
        return str(value)[:10] if value else None

    def post_process(self, row: dict, context: dict | None = None) -> dict:
        context = context or {}
        row["base"] = str(context.get("base", "")).upper()
        row["quote"] = str(context.get("quote", "")).upper()
        return {key: row.get(key) for key in self.schema["properties"]}
