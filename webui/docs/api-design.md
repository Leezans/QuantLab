# WebUI API Contract

Base URL: `/`

## Health
- `GET /health`

## Binance data
- `GET /api/binance/symbols`
- `GET /api/binance/klines`
  - query: `symbol,start,end,interval,market,style,preview_rows,fetch_checksum,verify_checksum`
- `GET /api/binance/trades`
  - query: `symbol,start,end,market,style,preview_rows,fetch_checksum,verify_checksum`
- `GET /api/binance/volume_profile`
  - query: `symbol,start,end,market,style,bins,volume_type,normalize,preview_rows`

All responses are JSON DTOs and exposed in OpenAPI for type generation.
