import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { TooltipProvider } from "@radix-ui/react-tooltip";
import { expect, test } from "vitest";
import { App } from "./App";

test("renders the speculative queue with non-color status text", async () => {
  render(<QueryClientProvider client={new QueryClient()}><TooltipProvider><App /></TooltipProvider></QueryClientProvider>);
  expect(await screen.findByText("Speculative queue")).toBeInTheDocument();
  expect(screen.getByText("Validation passed")).toBeInTheDocument();
  expect(screen.getAllByText("Running").length).toBeGreaterThan(0);
});
