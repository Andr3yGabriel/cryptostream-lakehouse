"use client";

import { useEffect, useRef, useState } from "react";
import { createChart, ColorType, CandlestickSeries } from "lightweight-charts";

// Map of available cryptocurrencies
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

// Helper function to format timestamp nicely in User's Local Time
const formatTime = (isoString: string) => {
  const cleanString = isoString.endsWith("Z") ? isoString : `${isoString}Z`;
  
  const date = new Date(cleanString);

  return date.toLocaleTimeString(undefined, { 
    hour: '2-digit', 
    minute: '2-digit', 
    second: '2-digit',
    hour12: false
  });
};

export default function Home() {
  const [metrics, setMetrics] = useState<any[]>([]);
  const [alerts, setAlerts] = useState<any[]>([]); // New State for Alerts
  const [selectedCoin, setSelectedCoin] = useState<string>("bitcoin");
  const chartContainerRef = useRef<HTMLDivElement>(null);

  const currentCoin = CRYPTO_OPTIONS.find((c) => c.id === selectedCoin) || CRYPTO_OPTIONS[0];

  // 1. Data Fetching Effect (Metrics + Alerts)
  useEffect(() => {
    const fetchData = () => {
      // Fetch Chart Metrics
      fetch(`http://127.0.0.1:8000/metrics/${selectedCoin}`)
        .then((res) => res.json())
        .then((data) => setMetrics(data))
        .catch((err) => console.error("Metrics API Error:", err));

      // Fetch Volatility Alerts (New Endpoint)
      fetch(`http://127.0.0.1:8000/alerts`)
        .then((res) => res.json())
        .then((data) => setAlerts(data))
        .catch((err) => console.error("Alerts API Error:", err));
    };

    setMetrics([]);
    fetchData(); 
    const interval = setInterval(fetchData, 10000); 
    return () => clearInterval(interval);
  }, [selectedCoin]); 

  // 2. Chart Rendering Effect
  useEffect(() => {
    if (!chartContainerRef.current || metrics.length === 0) return;

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" }, 
        textColor: "#94a3b8", 
      },
      width: chartContainerRef.current.clientWidth,
      height: 450,
      timeScale: {
        timeVisible: true, 
        secondsVisible: false,
        borderColor: "#334155", 
      },
      rightPriceScale: {
        borderColor: "#334155",
      },
      grid: {
        vertLines: { color: "rgba(51, 65, 85, 0.4)" }, 
        horzLines: { color: "rgba(51, 65, 85, 0.4)" },
      },
    });

    const isSmallCoin = ['dogecoin', 'cardano', 'ripple', 'tron', 'polkadot', 'avalanche-2', 'chainlink'].includes(selectedCoin);
    const decimalPlaces = isSmallCoin ? 4 : 2;
    const minMovement = isSmallCoin ? 0.0001 : 0.01;

    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#10b981", 
      downColor: "#ef4444", 
      borderVisible: false,
      wickUpColor: "#10b981",
      wickDownColor: "#ef4444",
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
      <div className="max-w-7xl mx-auto">
        
        <header className="mb-8 flex flex-col md:flex-row justify-between items-center gap-4">
          <div>
            <h1 className="text-4xl font-extrabold mb-2 bg-linear-to-r from-blue-400 via-emerald-400 to-teal-300 bg-clip-text text-transparent">
                CryptoStream Lakehouse
            </h1>
            <p className="text-slate-400 text-sm">
                Real-Time Analytical Dashboard • Gold Layer (Delta Lake)
            </p>
          </div>
          
          {/* Coin Selector moved to Header for cleaner layout */}
          <div className="w-full md:w-auto">
              <select
                value={selectedCoin}
                onChange={(e) => setSelectedCoin(e.target.value)}
                className="w-full md:w-64 bg-slate-900 border border-slate-700 text-slate-200 text-sm rounded-xl focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500 block p-3 outline-none cursor-pointer"
              >
                {CRYPTO_OPTIONS.map((coin) => (
                  <option key={coin.id} value={coin.id}>
                    {coin.name} ({coin.symbol})
                  </option>
                ))}
              </select>
          </div>
        </header>

        {/* GRID LAYOUT: Chart on Left, Alerts on Right */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            
            {/* LEFT COLUMN: Main Chart (Span 2 columns) */}
            <div className="lg:col-span-2 bg-slate-900/50 backdrop-blur-md border border-slate-800 rounded-2xl p-6 shadow-2xl h-142 flex flex-col">
                <div className="flex items-center gap-3 mb-4">
                    <div className="w-10 h-10 rounded-full bg-slate-800 flex items-center justify-center text-xl text-emerald-400">
                        {currentCoin.icon}
                    </div>
                    <div>
                        <h2 className="text-xl font-bold text-slate-100">{currentCoin.name}</h2>
                        <div className="flex items-center gap-2 text-xs text-emerald-400 font-medium">
                            <span className="relative flex h-2 w-2">
                                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                                <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
                            </span>
                            LIVE MARKET
                        </div>
                    </div>
                </div>

                <div className="flex-1 bg-slate-950/50 rounded-xl border border-slate-800/50 relative overflow-hidden">
                    {metrics.length === 0 ? (
                        <div className="absolute inset-0 flex items-center justify-center text-slate-500 gap-2">
                            <svg className="animate-spin h-6 w-6 text-emerald-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                            </svg>
                            <span className="text-sm font-medium">Syncing Spark...</span>
                        </div>
                    ) : (
                        <div ref={chartContainerRef} className="w-full h-full" />
                    )}
                </div>
            </div>

            {/* RIGHT COLUMN: Alert Feed (Span 1 column) */}
            <div className="lg:col-span-1 bg-slate-900/50 backdrop-blur-md border border-slate-800 rounded-2xl p-6 shadow-xl h-142 flex flex-col">
                <div className="flex items-center justify-between mb-4">
                    <h3 className="text-lg font-bold text-slate-100 flex items-center gap-2">
                        <span>🚨</span> Volatility Alerts
                    </h3>
                    <span className="text-xs text-slate-500 px-2 py-1 bg-slate-800 rounded-md">Last 20 Events</span>
                </div>

                <div className="flex-1 overflow-y-auto pr-2 space-y-3 scrollbar-thin scrollbar-thumb-slate-700 scrollbar-track-slate-800/30">
                    {alerts.length === 0 ? (
                        <div className="text-center text-slate-500 text-sm mt-10">
                            No volatility alerts detected yet. <br/> Market is stable.
                        </div>
                    ) : (
                        alerts.map((alert) => (
                            <div key={alert._id} className="p-3 bg-slate-950/80 border border-slate-800 rounded-lg hover:border-emerald-500/30 transition-colors group">
                                <div className="flex justify-between items-start mb-1">
                                    <div className="flex items-center gap-2">
                                        <span className="font-bold text-slate-200 capitalize">{alert.coin_id}</span>
                                        <span className="text-xs text-orange-400 bg-orange-400/10 px-1.5 py-0.5 rounded">
                                            {alert.volatility}% Vol
                                        </span>
                                    </div>
                                    <span className="text-xs text-slate-500 font-mono">
                                        {formatTime(alert.timestamp)}
                                    </span>
                                </div>
                                <div className="text-xs text-slate-400 grid grid-cols-2 gap-y-1">
                                    <span>Avg: <span className="text-slate-300">${alert.avg_price.toFixed(alert.avg_price < 1 ? 4 : 2)}</span></span>
                                    <span>Max: <span className="text-emerald-400">${alert.max_price.toFixed(alert.avg_price < 1 ? 4 : 2)}</span></span>
                                </div>
                            </div>
                        ))
                    )}
                </div>
            </div>

        </div>
      </div>
    </main>
  );
}