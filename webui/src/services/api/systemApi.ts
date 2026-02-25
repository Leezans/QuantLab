import type { HealthResponseDTO } from "@/types/dto";

import { httpClient } from "@/services/http/client";

export async function fetchHealth(): Promise<HealthResponseDTO> {
  const response = await httpClient.get<HealthResponseDTO>("/health");
  return response.data;
}
