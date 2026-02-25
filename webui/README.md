# QuantLab WebUI

React + TypeScript WebUI for QuantLab.

## Stack
- React 18 + TypeScript
- React Router
- Ant Design
- TradingView lightweight-charts
- Axios

## Project structure
- `src/pages`: route pages
- `src/components`: reusable UI and chart wrappers
- `src/services`: HTTP client and API modules
- `src/types`: generated and shared DTO types

## Run
```bash
cd webui
npm install
npm run dev
```

## Type generation from FastAPI OpenAPI
```bash
cd webui
npm run gen:types
```

You can override OpenAPI URL:
```bash
OPENAPI_URL=http://127.0.0.1:8000/openapi.json npm run gen:types
```

## Build and test
```bash
npm run lint
npm run test
npm run build
```
