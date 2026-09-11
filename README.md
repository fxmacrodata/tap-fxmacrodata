# tap-fxmacrodata

A [Singer](https://www.singer.io/) tap for [FXMacroData](https://fxmacrodata.com/?utm_source=github-tap-fxmacrodata&utm_medium=referral&utm_campaign=open_source_integrations&utm_content=readme), built with the Meltano SDK.

FXMacroData aggregates official publishers — central banks and national
statistics agencies — across 18 currencies behind one contract, and stamps every
observation with the instant it was published.

That last part is why this tap exists. Most macro sources give you a value and a
reference date, which quietly invites lookahead: the figure for March was not
knowable in March. Every `announcements` record here carries
`announcement_datetime`, the instant the figure actually went out, so a model
trained downstream can filter on when a number was *known* rather than when it
*referred to*.

## Streams

| Stream | Grain | Replication |
| --- | --- | --- |
| `announcements` | one record per release of one indicator | incremental on `date` |
| `release_calendar` | scheduled and recent releases | full table |
| `data_catalogue` | indicators a currency publishes | full table |
| `forex_rates` | daily reference rate for a pair | incremental on `date` |

`announcements` and `forex_rates` are partitioned per series, so state is kept
per currency/indicator and per pair. Interrupting a sync re-runs only the series
that had not finished.

## Install

```bash
pipx install git+https://github.com/fxmacrodata/tap-fxmacrodata.git
```

Or with Meltano:

```bash
meltano add extractor tap-fxmacrodata
```

## Configuration

| Setting | Required | Description |
| --- | --- | --- |
| `api_key` | no | Widens access beyond public USD data |
| `currencies` | no | Defaults to `["USD"]` |
| `indicators` | no | Indicator slugs; unset skips `announcements` |
| `fx_pairs` | no | Pairs like `EUR/USD`; unset skips `forex_rates` |
| `start_date` | no | Earliest reference date for dated streams |
| `api_url` | no | Override the API base URL |

Run `tap-fxmacrodata --about --format=markdown` for the full settings list.

**USD macro data is public**, so the tap runs with no key at all — useful for
trying it before deciding anything:

```json
{
  "currencies": ["USD"],
  "indicators": ["inflation", "policy_rate", "unemployment"],
  "start_date": "2020-01-01T00:00:00Z"
}
```

A key covers the other seventeen currencies, the full history, and FX. It is
sent as an `X-API-Key` header, so it never lands in a URL or a request log.
[Subscribe](https://fxmacrodata.com/subscribe?utm_source=github-tap-fxmacrodata&utm_medium=referral&utm_campaign=open_source_integrations&utm_content=subscribe).

### Finding indicators

Indicator slugs differ by currency. Sync `data_catalogue` first:

```bash
tap-fxmacrodata --config config.json --select data_catalogue
```

## A currency you are not subscribed to

The tap logs a warning and moves on rather than failing the run. Access is a
configuration fact, not a transport error, and stopping the whole sync would
make a mixed currency list unusable.

## Usage

```bash
tap-fxmacrodata --config config.json > output.jsonl
tap-fxmacrodata --config config.json --discover > catalog.json
```

## Development

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
pytest
```

## Links

- [API reference](https://fxmacrodata.com/documentation/reference?utm_source=github-tap-fxmacrodata&utm_medium=referral&utm_campaign=open_source_integrations&utm_content=docs)
- [Singer specification](https://github.com/singer-io/getting-started)

MIT licensed.
