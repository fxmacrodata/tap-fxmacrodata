"""Shared REST behaviour for every FXMacroData stream."""

from __future__ import annotations

import typing as t

import requests
from singer_sdk.pagination import BaseAPIPaginator, SinglePagePaginator
from singer_sdk.streams import RESTStream

API_URL = "https://api.fxmacrodata.com/v1"

# The API caps a page at 100 rows on every paginated endpoint.
PAGE_SIZE = 100


class FXMacroDataStream(RESTStream):
    """Base stream.

    USD macro data is public, so the tap runs without an API key and simply
    returns less. A key widens it to the other seventeen currencies and the
    full history, and travels as a header so it never reaches a request log.
    """

    records_jsonpath = "$.data[*]"

    @property
    def url_base(self) -> str:
        return self.config.get("api_url", API_URL)

    @property
    def http_headers(self) -> dict:
        """Send the key as a header so it never reaches a URL or a request log.

        Set here rather than through an authenticator because the key is
        optional: public USD data needs none, and an authenticator that returns
        nothing is not something the SDK can apply.
        """
        headers = {"Accept": "application/json"}
        api_key = self.config.get("api_key")
        if api_key:
            headers["X-API-Key"] = api_key
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        return SinglePagePaginator()

    def get_url_params(
        self, context: dict | None, next_page_token: t.Any  # noqa: ANN401
    ) -> dict[str, t.Any]:
        return {"limit": PAGE_SIZE}

    def validate_response(self, response: requests.Response) -> None:
        """Treat an unsubscribed currency as empty rather than as a failure.

        A 401 here means the key does not cover this currency, which is a
        configuration fact rather than a transport error. Failing the whole tap
        on it would make a mixed currency list unusable.
        """
        if response.status_code in (401, 403):
            self.logger.warning(
                "%s is not available with the configured access; skipping. "
                "See https://fxmacrodata.com/subscribe"
                "?utm_source=meltano&utm_medium=referral"
                "&utm_campaign=open_source_integrations&utm_content=tap_log",
                response.request.url.split("?")[0] if response.request.url else self.path,
            )
            return
        if response.status_code == 404:
            self.logger.info("No series published at %s; skipping.", self.path)
            return
        super().validate_response(response)

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        if response.status_code in (401, 403, 404):
            return iter([])
        return super().parse_response(response)
