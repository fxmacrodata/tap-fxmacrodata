"""Behaviour that is easy to get wrong and expensive to get wrong quietly."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from tap_fxmacrodata.tap import TapFXMacroData

BASE_CONFIG = {"currencies": ["USD"], "indicators": ["inflation"]}


def _tap(**overrides):
    return TapFXMacroData(config={**BASE_CONFIG, **overrides}, parse_env_config=False)


def _stream(name: str, **overrides):
    return next(s for s in _tap(**overrides).discover_streams() if s.name == name)


def _response(payload, status=200):
    return SimpleNamespace(
        status_code=status,
        json=lambda: payload,
        request=SimpleNamespace(url="https://api.fxmacrodata.com/v1/test"),
        headers={},
        text=json.dumps(payload),
    )


class TestCredentialHandling:
    def test_the_key_is_a_header_never_a_query_parameter(self):
        stream = _stream("announcements", api_key="test-key")

        headers = stream.http_headers
        params = stream.get_url_params({"currency": "USD", "indicator": "inflation"}, None)

        assert headers["X-API-Key"] == "test-key"
        assert "test-key" not in json.dumps(params)

    def test_no_auth_header_without_a_key(self):
        # Public USD data must work with no credential at all, so an absent key
        # has to be an ordinary state rather than an error.
        assert "X-API-Key" not in _stream("announcements").http_headers


class TestPartitioning:
    def test_one_partition_per_currency_and_indicator(self):
        stream = _stream("announcements", currencies=["USD", "EUR"], indicators=["inflation", "gdp"])

        assert stream.partitions == [
            {"currency": "USD", "indicator": "inflation"},
            {"currency": "USD", "indicator": "gdp"},
            {"currency": "EUR", "indicator": "inflation"},
            {"currency": "EUR", "indicator": "gdp"},
        ]

    def test_announcements_are_skipped_when_no_indicator_is_chosen(self):
        # Indicator slugs differ per currency, so there is no safe default.
        assert _stream("announcements", indicators=[]).partitions is None

    def test_currencies_are_upper_cased(self):
        assert _stream("release_calendar", currencies=["usd"]).partitions == [{"currency": "USD"}]

    def test_fx_pairs_split_into_base_and_quote(self):
        stream = _stream("forex_rates", fx_pairs=["EUR/USD", "eur/jpy"])

        assert stream.partitions == [
            {"base": "eur", "quote": "usd"},
            {"base": "eur", "quote": "jpy"},
        ]

    def test_fx_is_skipped_when_no_pair_is_chosen(self):
        assert _stream("forex_rates").partitions is None


class TestIncrementalCursor:
    def test_the_configured_start_date_becomes_a_plain_date(self):
        stream = _stream("announcements", start_date="2020-01-01T00:00:00Z")

        params = stream.get_url_params({"currency": "USD", "indicator": "inflation"}, None)

        # The API takes YYYY-MM-DD; sending a full timestamp is rejected.
        assert params["start_date"] == "2020-01-01"

    def test_no_start_date_means_no_filter(self):
        params = _stream("announcements").get_url_params({"currency": "USD"}, None)

        assert "start_date" not in params

    def test_the_page_size_matches_the_api_cap(self):
        assert _stream("announcements").get_url_params(None, None)["limit"] == 100


class TestRecordShape:
    def test_series_identity_is_stamped_onto_each_row(self):
        # Currency and indicator travel in the path, not the record, so without
        # this a table of several series could not be told apart.
        stream = _stream("announcements")

        row = stream.post_process({"announcement_id": "usd_inflation_2026-07-31", "val": 3.4},
                                  {"currency": "USD", "indicator": "inflation"})

        assert row["currency"] == "USD"
        assert row["indicator"] == "inflation"
        assert row["val"] == 3.4

    def test_fx_rows_carry_the_pair(self):
        stream = _stream("forex_rates", fx_pairs=["EUR/USD"])

        row = stream.post_process({"date": "2026-09-10", "val": 1.1616},
                                  {"base": "eur", "quote": "usd"})

        assert row == {"base": "EUR", "quote": "USD", "date": "2026-09-10", "val": 1.1616}

    def test_the_catalogue_is_an_object_keyed_by_indicator(self):
        # Unlike every other endpoint, this one returns no data array.
        stream = _stream("data_catalogue")

        records = list(_stream_parse(stream, {"inflation": {"source": "BLS", "unit": "%"}}))

        assert records == [{"indicator": "inflation", "source": "BLS", "unit": "%"}]


def _stream_parse(stream, payload, status=200):
    return stream.parse_response(_response(payload, status))


class TestDegradedAccess:
    @pytest.mark.parametrize("status", [401, 403])
    def test_an_unsubscribed_currency_yields_nothing_instead_of_failing(self, status):
        # Access is a configuration fact, not a transport error. Failing the run
        # would make a mixed currency list unusable.
        stream = _stream("data_catalogue")

        stream.validate_response(_response({"detail": "subscription required"}, status))

        assert list(_stream_parse(stream, {"inflation": {}}, status)) == []

    def test_a_missing_series_yields_nothing(self):
        stream = _stream("data_catalogue")

        stream.validate_response(_response({"detail": "not found"}, 404))

        assert list(_stream_parse(stream, {"inflation": {}}, 404)) == []

    def test_a_server_error_still_raises(self):
        # Degrading on 5xx would silently produce an incomplete warehouse.
        stream = _stream("data_catalogue")

        with pytest.raises(Exception):
            stream.validate_response(_response({"detail": "boom"}, 500))


class TestTapContract:
    def test_every_stream_is_discovered(self):
        assert {s.name for s in _tap().discover_streams()} == {
            "announcements",
            "data_catalogue",
            "forex_rates",
            "release_calendar",
        }

    def test_each_stream_declares_a_primary_key(self):
        for stream in _tap().discover_streams():
            assert stream.primary_keys, f"{stream.name} has no primary key"

    def test_the_api_key_setting_is_marked_secret(self):
        # Otherwise it would be echoed by --about and in Meltano's UI.
        assert TapFXMacroData.config_jsonschema["properties"]["api_key"].get("secret") is True
