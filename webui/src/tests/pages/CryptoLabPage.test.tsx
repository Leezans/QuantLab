import { render, screen, waitFor } from "@testing-library/react";

import { CryptoLabPage } from "@/pages/labs/CryptoLabPage";

jest.mock("@/services/api/marketDataApi", () => ({
  fetchSymbols: jest.fn().mockResolvedValue(["BTCUSDT", "ETHUSDT"]),
  fetchKlines: jest.fn(),
  fetchTrades: jest.fn(),
  fetchVolumeProfile: jest.fn(),
}));

describe("CryptoLabPage", () => {
  it("renders top level tabs", async () => {
    render(<CryptoLabPage />);

    await waitFor(() => {
      expect(screen.getByText("CryptoLab")).toBeInTheDocument();
      expect(screen.getByText("Data")).toBeInTheDocument();
      expect(screen.getByText("Factors")).toBeInTheDocument();
      expect(screen.getByText("Explorer")).toBeInTheDocument();
    });
  });
});
