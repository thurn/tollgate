import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, beforeEach, expect, test } from "vitest";
import { App } from "./App";

beforeEach(() => localStorage.clear());
afterEach(cleanup);

test("combines active and completed gate runs with non-color status text", async () => {
  render(<QueryClientProvider client={new QueryClient()}><App /></QueryClientProvider>);
  expect(await screen.findByRole("heading", { name: "Runs" })).toBeInTheDocument();
  expect(screen.getByRole("alert")).toHaveTextContent("Master push failed");
  expect(screen.getByRole("alert")).toHaveTextContent("Step trox failed");
  expect(screen.getByRole("button", { name: /View failure/ })).toBeInTheDocument();
  expect(screen.getByText("Validation passed")).toBeInTheDocument();
  expect(screen.getAllByText("Running").length).toBeGreaterThan(0);
  expect(await screen.findByText("Keep candidate evidence after restart")).toBeInTheDocument();
  expect(screen.getByText("Retire legacy promotion script")).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: /History/ })).not.toBeInTheDocument();
  expect(screen.queryByText("Approve")).not.toBeInTheDocument();
  expect(screen.queryByText("Add repository")).not.toBeInTheDocument();
});

test("shows independent checks on their own tab", async () => {
  render(<QueryClientProvider client={new QueryClient()}><App /></QueryClientProvider>);
  fireEvent.click(await screen.findByRole("button", { name: "Checks" }));
  expect(screen.getByRole("heading", { name: "Checks" })).toBeInTheDocument();
  expect(screen.getByText("Verify dependency graph changes")).toBeInTheDocument();
  expect(await screen.findByText("Audit runner environment capture")).toBeInTheDocument();
  expect(screen.queryByText("Make promotion intents crash-safe")).not.toBeInTheDocument();
});
