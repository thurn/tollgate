import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { expect, test } from "vitest";
import { App } from "./App";

test("renders the release queue with non-color status text", async () => {
  render(<QueryClientProvider client={new QueryClient()}><App /></QueryClientProvider>);
  expect(await screen.findByRole("heading", { name: "Queue" })).toBeInTheDocument();
  expect(screen.getByText("Validation passed")).toBeInTheDocument();
  expect(screen.getAllByText("Running").length).toBeGreaterThan(0);
  expect(screen.queryByText("Approve")).not.toBeInTheDocument();
  expect(screen.queryByText("Check")).not.toBeInTheDocument();
  expect(screen.queryByText("Add repository")).not.toBeInTheDocument();
});
