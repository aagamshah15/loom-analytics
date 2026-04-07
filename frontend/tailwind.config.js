/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        loom: {
          bg: "#FAFAF9",
          paper: "#FFFFFF",
          stone: "#E7E5E4",
          ink: "#1C1917",
          muted: "#78716C",
          accent: "#C2410C",
          espresso: "#292524",
          cream: "#FAFAF9",
        },
      },
      fontFamily: {
        sans: ["Inter", "Geist", "system-ui", "sans-serif"],
        display: ["Merriweather", "Georgia", "serif"],
      },
      boxShadow: {
        panel: "0 18px 60px rgba(28, 25, 23, 0.08)",
        loom: "0 24px 80px rgba(28, 25, 23, 0.12)",
      },
      borderRadius: {
        panel: "1rem",
      },
    },
  },
  plugins: [],
};
