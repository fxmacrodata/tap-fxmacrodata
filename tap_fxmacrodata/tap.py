"""FXMacroData tap."""

from __future__ import annotations

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_fxmacrodata.streams import (
    AnnouncementsStream,
    DataCatalogueStream,
    ForexRatesStream,
    ReleaseCalendarStream,
)

STREAMS: list[type[Stream]] = [
    AnnouncementsStream,
    DataCatalogueStream,
    ForexRatesStream,
    ReleaseCalendarStream,
]


class TapFXMacroData(Tap):
    """Extract official macroeconomic, FX and release-calendar data."""

    name = "tap-fxmacrodata"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            secret=True,
            description=(
                "FXMacroData API key. USD macro data is public, so the tap runs "
                "without one; a key widens it to the other seventeen currencies, "
                "the full history, and FX."
            ),
        ),
        th.Property(
            "currencies",
            th.ArrayType(th.StringType),
            default=["USD"],
            description="Currency codes to extract, for example [\"USD\", \"EUR\"].",
        ),
        th.Property(
            "indicators",
            th.ArrayType(th.StringType),
            description=(
                "Indicator slugs for the announcements stream, for example "
                "[\"inflation\", \"policy_rate\"]. Sync the data_catalogue stream "
                "first to see what a currency publishes. Leave unset to skip "
                "announcements."
            ),
        ),
        th.Property(
            "fx_pairs",
            th.ArrayType(th.StringType),
            description=(
                "Currency pairs for the forex_rates stream, for example "
                "[\"EUR/USD\"]. Leave unset to skip FX."
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest reference date to extract for dated streams.",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.fxmacrodata.com/v1",
            description="Override the API base URL. Rarely needed.",
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        return [stream_class(tap=self) for stream_class in STREAMS]


if __name__ == "__main__":
    TapFXMacroData.cli()
