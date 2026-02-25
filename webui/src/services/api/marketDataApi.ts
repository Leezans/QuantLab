import type { KlinesResponseDTO, TradesResponseDTO, VolumeProfileResponseDTO } from "@/types/dto";
import type { KlinesRequest, TradesRequest, VolumeProfileRequest } from "@/types/requests";

import { httpClient } from "@/services/http/client";

export async function fetchSymbols(): Promise<string[]> {
  const response = await httpClient.get<string[]>("/api/binance/symbols");
  return response.data;
}

export async function fetchKlines(params: KlinesRequest): Promise<KlinesResponseDTO> {
  const response = await httpClient.get<KlinesResponseDTO>("/api/binance/klines", { params });
  return response.data;
}

export async function fetchTrades(params: TradesRequest): Promise<TradesResponseDTO> {
  const response = await httpClient.get<TradesResponseDTO>("/api/binance/trades", { params });
  return response.data;
}

export async function fetchVolumeProfile(
  params: VolumeProfileRequest,
): Promise<VolumeProfileResponseDTO> {
  const response = await httpClient.get<VolumeProfileResponseDTO>("/api/binance/volume_profile", {
    params,
  });
  return response.data;
}
