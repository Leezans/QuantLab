export interface BaseRangeRequest {
  symbol: string;
  start: string;
  end: string;
  market: "spot" | "futures";
  style: "mirror" | "hive";
  preview_rows?: number;
}

export interface KlinesRequest extends BaseRangeRequest {
  interval: "1m" | "5m" | "1h" | "1d";
}

export type TradesRequest = BaseRangeRequest;

export interface VolumeProfileRequest extends BaseRangeRequest {
  bins: number;
  volume_type: "base" | "quote";
  normalize: boolean;
}
