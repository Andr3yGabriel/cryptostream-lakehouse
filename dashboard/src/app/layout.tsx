import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

// Configure standard Next.js fonts
const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

// Update global metadata for SEO and browser tabs
export const metadata: Metadata = {
  title: "CryptoStream Lakehouse",
  description: "Real-time cryptocurrency analytical dashboard powered by Delta Lake and DuckDB.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      {/* Apply global dark background and text colors to the body 
        to prevent white flashes and ensure consistent styling 
      */}
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased bg-slate-950 text-slate-50`}
      >
        {children}
      </body>
    </html>
  );
}