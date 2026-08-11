import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { TooltipProvider } from "@radix-ui/react-tooltip";
import { App } from "./app/App";
import "./styles/index.css";

const client = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 1_000, refetchOnWindowFocus: true, retry: 2 },
    mutations: { retry: 0 },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={client}>
      <TooltipProvider delayDuration={450}>
        <App />
      </TooltipProvider>
    </QueryClientProvider>
  </StrictMode>,
);

