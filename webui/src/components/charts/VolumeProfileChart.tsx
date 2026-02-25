import { useEffect, useMemo, useRef } from "react";
import { ColorType, createChart, type HistogramData, type IChartApi, type Time } from "lightweight-charts";

import type { VolumeProfileBinDTO } from "@/types/dto";

interface VolumeProfileChartProps {
  data: VolumeProfileBinDTO[];
  height?: number;
}

export function VolumeProfileChart({ data, height = 280 }: VolumeProfileChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);

  const histogramData = useMemo<HistogramData<Time>[]>(() => {
    const base = 1_700_000_000;
    return data.map((item, index) => ({
      time: (base + index * 60) as Time,
      value: item.volume,
      color: "rgba(139,92,246,0.65)",
    }));
  }, [data]);

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }

    const chart = createChart(containerRef.current, {
      height,
      layout: {
        background: { type: ColorType.Solid, color: "#ffffff" },
        textColor: "#334155",
      },
      grid: {
        vertLines: { color: "rgba(148,163,184,0.2)" },
        horzLines: { color: "rgba(148,163,184,0.2)" },
      },
      rightPriceScale: {
        borderColor: "#e2e8f0",
      },
      timeScale: {
        borderColor: "#e2e8f0",
        timeVisible: false,
      },
      handleScroll: false,
      handleScale: false,
    });

    const series = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      base: 0,
    });
    series.setData(histogramData);
    chart.timeScale().fitContent();
    chartRef.current = chart;

    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) {
        return;
      }
      chart.applyOptions({ width: entry.contentRect.width });
    });
    observer.observe(containerRef.current);

    return () => {
      observer.disconnect();
      chart.remove();
      chartRef.current = null;
    };
  }, [histogramData, height]);

  return <div ref={containerRef} className="chart-container" style={{ height }} />;
}
