"use client";

import { useEffect, useRef, useState } from "react";
import { createChart, ColorType, CandlestickSeries } from "lightweight-charts";

// Map of available cryptocurrencies matching our Kafka/Spark pipeline
const CRYPTO_OPTIONS = [
  { id: "bitcoin", name: "Bitcoin", symbol: "BTC", icon: "₿" },
  { id: "ethereum", name: "Ethereum", symbol: "ETH", icon: "⬨" },
  { id: "solana", name: "Solana", symbol: "SOL", icon: "◎" },
  { id: "cardano", name: "Cardano", symbol: "ADA", icon: "₳" },
  { id: "ripple", name: "Ripple", symbol: "XRP", icon: "✕" },
  { id: "dogecoin", name: "Dogecoin", symbol: "DOGE", icon: "Ð" },
  { id: "polkadot", name: "Polkadot", symbol: "DOT", icon: "●" },
  { id: "chainlink", name: "Chainlink", symbol: "LINK", icon: "🔗" },
  { id: "avalanche-2", name: "Avalanche", symbol: "AVAX", icon: "🔺" },
  { id: "tron", name: "Tron", symbol: "TRX", icon: "♦" },
];

export default function Home() {
  const [metrics, setMetrics] = useState<any[]>([]);
  const [selectedCoin, setSelectedCoin] = useState<string>("bitcoin");
  const chartContainerRef = useRef<HTMLDivElement>(null);

  // Get current coin details for UI rendering
  const currentCoin = CRYPTO_OPTIONS.find((c) => c.id === selectedCoin) || CRYPTO_OPTIONS[0];

  // 1. Data Fetching Effect
  useEffect(() => {
    const fetchData = () => {
      fetch(`http://127.0.0.1:8000/metrics/${selectedCoin}`)
        .then((res) => res.json())
        .then((data) => setMetrics(data))
        .catch((err) => console.error("API Error:", err));
    };

    // Clear previous metrics when switching coins to show loading state
    setMetrics([]);
    fetchData(); 
    
    // Setup polling every 10 seconds
    const interval = setInterval(fetchData, 10000); 
    
    // Cleanup interval on unmount or when selectedCoin changes
    return () => clearInterval(interval);
  }, [selectedCoin]); // Trigger effect whenever the selected coin changes

  // 2. Chart Rendering Effect
  useEffect(() => {
    if (!chartContainerRef.current || metrics.length === 0) return;

    // Initialize chart with a cleaner, modern dark theme
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" }, 
        textColor: "#94a3b8", // slate-400
      },
      width: chartContainerRef.current.clientWidth,
      height: 450,
      timeScale: {
        timeVisible: true, 
        secondsVisible: false,
        borderColor: "#334155", // slate-700
      },
      rightPriceScale: {
        borderColor: "#334155",
      },
      grid: {
        vertLines: { color: "rgba(51, 65, 85, 0.4)" }, // subtle slate-700
        horzLines: { color: "rgba(51, 65, 85, 0.4)" },
      },
      crosshair: {
        mode: 1, // Normal crosshair mode
        vertLine: { color: "#94a3b8", style: 3 },
        horzLine: { color: "#94a3b8", style: 3 },
      },
    });

    // Determine decimal precision based on the coin type
    // Bitcoin and Ethereum use 2 decimals, smaller altcoins use 4
    const isSmallCoin = ['dogecoin', 'cardano', 'ripple', 'tron', 'polkadot'].includes(selectedCoin);
    const decimalPlaces = isSmallCoin ? 4 : 2;
    const minMovement = isSmallCoin ? 0.0001 : 0.01;

    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#10b981", // emerald-500
      downColor: "#ef4444", // red-500
      borderVisible: false,
      wickUpColor: "#10b981",
      wickDownColor: "#ef4444",
      // Implement custom price formatter using native JS Intl API
      priceFormat: {
        type: 'custom',
        minMove: minMovement,
        formatter: (price: number) => {
          return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            minimumFractionDigits: decimalPlaces,
            maximumFractionDigits: decimalPlaces,
          }).format(price);
        },
      },
    });
    
    const reversedMetrics = [...metrics].reverse();

    const formattedData = reversedMetrics.map((item, index) => {
      const rawEpoch = Math.floor(item.window_start);
      const timezoneOffset = new Date().getTimezoneOffset() * 60; 
      const localTimeInSeconds = rawEpoch - timezoneOffset;
      
      const openPrice = index === 0 ? item.avg_price : reversedMetrics[index - 1].avg_price;
      const actualHigh = Math.max(item.max_price, openPrice, item.avg_price);
      const actualLow = Math.min(item.min_price, openPrice, item.avg_price);

      return {
        time: localTimeInSeconds as any,
        open: openPrice,         
        high: actualHigh,        
        low: actualLow,          
        close: item.avg_price,   
      };
    });

    candlestickSeries.setData(formattedData);
    chart.timeScale().fitContent();

    // Handle window resize dynamically
    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current?.clientWidth });
    };
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
    };
  }, [metrics]);

  return (
    <main className="min-h-screen bg-slate-950 text-white p-4 md:p-8 font-sans selection:bg-emerald-500/30">
      <div className="max-w-6xl mx-auto">
        
        <header className="mb-8 text-center md:text-left">
          <h1 className="text-4xl md:text-5xl font-extrabold mb-3 bg-linear-to-r from-blue-400 via-emerald-400 to-teal-300 bg-clip-text text-transparent">
            CryptoStream Lakehouse
          </h1>
          <p className="text-slate-400 text-sm md:text-base max-w-2xl">
            Real-Time Analytical Dashboard • Gold Layer (Delta Lake)
          </p>
        </header>

        {/* Main Dashboard Card with Glassmorphism */}
        <div className="bg-slate-900/50 backdrop-blur-md border border-slate-800 rounded-2xl p-4 md:p-6 shadow-2xl">
          
          {/* Controls Header */}
          <div className="flex flex-col md:flex-row justify-between items-start md:items-center mb-8 gap-4">
            
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center text-2xl text-emerald-400 shadow-inner">
                {currentCoin.icon}
              </div>
              <div>
                <h2 className="text-2xl font-bold text-slate-100">
                  {currentCoin.name} <span className="text-slate-500 font-medium text-lg ml-1">({currentCoin.symbol})</span>
                </h2>
                <div className="flex items-center gap-2 text-xs text-emerald-400 font-medium tracking-wide mt-1">
                  <span className="relative flex h-2.5 w-2.5">
                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                    <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-emerald-500"></span>
                  </span>
                  LIVE DATA ACTIVE
                </div>
              </div>
            </div>

            {/* Cryptocurrency Selector */}
            <div className="w-full md:w-auto">
              <label htmlFor="coin-select" className="sr-only">Select Cryptocurrency</label>
              <select
                id="coin-select"
                value={selectedCoin}
                onChange={(e) => setSelectedCoin(e.target.value)}
                className="w-full md:w-64 bg-slate-800 border border-slate-700 text-slate-200 text-sm rounded-xl focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500 block p-3 outline-none transition-all cursor-pointer hover:bg-slate-750"
              >
                {CRYPTO_OPTIONS.map((coin) => (
                  <option key={coin.id} value={coin.id}>
                    {coin.name} ({coin.symbol})
                  </option>
                ))}
              </select>
            </div>
          </div>

          {/* Chart Rendering Area */}
          <div className="bg-slate-950/50 rounded-xl p-2 md:p-4 border border-slate-800/50">
            {metrics.length === 0 ? (
              <div className="h-112.5 flex flex-col items-center justify-center text-slate-500 gap-3">
                <svg className="animate-spin h-8 w-8 text-emerald-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                <span className="text-sm font-medium tracking-wide animate-pulse">Waiting for Spark Streaming windows...</span>
              </div>
            ) : (
              <div ref={chartContainerRef} className="w-full rounded overflow-hidden" />
            )}
          </div>
          
        </div>
      </div>
    </main>
  );
}