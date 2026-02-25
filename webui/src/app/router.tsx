import { Navigate, createBrowserRouter } from "react-router-dom";

import { AppShell } from "@/components/layout/AppShell";
import { FuturesLabPage } from "@/pages/labs/FuturesLabPage";
import { CryptoLabPage } from "@/pages/labs/CryptoLabPage";
import { StocksLabPage } from "@/pages/labs/StocksLabPage";
import { NotFoundPage } from "@/pages/NotFoundPage";

export const appRouter = createBrowserRouter([
  {
    path: "/",
    element: <AppShell />,
    children: [
      { index: true, element: <Navigate to="/labs/crypto" replace /> },
      { path: "labs/crypto", element: <CryptoLabPage /> },
      { path: "labs/stocks", element: <StocksLabPage /> },
      { path: "labs/futures", element: <FuturesLabPage /> },
      { path: "*", element: <NotFoundPage /> },
    ],
  },
]);
