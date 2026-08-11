import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  clearScreen: false,
  server: { port: 1420, strictPort: true },
  envPrefix: ["VITE_", "TAURI_ENV_"],
  build: {
    target: "safari15",
    minify: "esbuild",
    sourcemap: true,
    rollupOptions: {
      output: {
        manualChunks: {
          motion: ["framer-motion"],
          radix: ["@radix-ui/react-dialog", "@radix-ui/react-progress", "@radix-ui/react-separator", "@radix-ui/react-tooltip"],
          query: ["@tanstack/react-query"],
        },
      },
    },
  },
});
