import axios from "axios";

import { getAccessToken } from "@/services/auth/tokenStorage";

const baseURL = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";

export const httpClient = axios.create({
  baseURL,
  timeout: 30_000,
});

httpClient.interceptors.request.use((config) => {
  const token = getAccessToken();
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});
