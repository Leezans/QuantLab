/* eslint-disable */
/**
 * This file can be regenerated from FastAPI OpenAPI spec.
 * Command: npm run gen:types
 */

export interface components {
  schemas: {
    PipelineStatsDTO: {
      total_days: number;
      ok: number;
      skipped: number;
      failed: number;
    };
    KlinePointDTO: {
      time: number;
      open: number;
      high: number;
      low: number;
      close: number;
      volume?: number | null;
    };
    TradePointDTO: {
      time: number;
      price: number;
      quantity?: number | null;
      quote_quantity?: number | null;
    };
    VolumeProfileBinDTO: {
      price: number;
      volume: number;
    };
    KlinesResponseDTO: {
      symbol: string;
      market: "spot" | "futures";
      interval: string;
      start: string;
      end: string;
      source: string;
      stats: components["schemas"]["PipelineStatsDTO"];
      row_count: number;
      parquet_paths: string[];
      errors: string[];
      preview: components["schemas"]["KlinePointDTO"][];
    };
    TradesResponseDTO: {
      symbol: string;
      market: "spot" | "futures";
      start: string;
      end: string;
      source: string;
      stats: components["schemas"]["PipelineStatsDTO"];
      row_count: number;
      parquet_paths: string[];
      errors: string[];
      preview: components["schemas"]["TradePointDTO"][];
    };
    VolumeProfileResponseDTO: {
      symbol: string;
      market: "spot" | "futures";
      start: string;
      end: string;
      bins: number;
      volume_type: "base" | "quote";
      normalized: boolean;
      profile: components["schemas"]["VolumeProfileBinDTO"][];
      trades_source: string;
      trades_row_count: number;
      errors: string[];
    };
    HealthResponseDTO: {
      status: string;
    };
  };
}
