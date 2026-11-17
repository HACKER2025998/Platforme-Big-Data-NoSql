import { defineConfig } from "vite";
export default defineConfig({
  build: {
    rollupOptions: {
      output: {
        manualChunks: { charts: ["recharts"], react: ["react", "react-dom"] },
      },
    },
  },
  server: { proxy: { "/api": "http://localhost:8080" } },
});
