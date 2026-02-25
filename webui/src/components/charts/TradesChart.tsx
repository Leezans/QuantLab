import { useEffect, useMemo, useRef } from "react";
import {
  ColorType,
  createChart,
  type HistogramData,
  type IChartApi,
  type LineData,
  type Time,
} from "lightweight-charts";

import type { TradePointDTO } from "@/types/dto";

interface TradesChartProps {
  data: TradePointDTO[];
  height?: number;
}

export function TradesChart({ data, height = 420 }: TradesChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);

  const lineData = useMemo<LineData[]>(
    () => data.map((item) => ({ time: item.time as Time, value: item.price })),
    [data],
  );

  const volumeData = useMemo<HistogramData<Time>[]>(
    () =>
      data
        .filter((item) => item.quantity !== undefined && item.quantity !== null)
        .map((item) => ({
          time: item.time as Time,
          value: item.quantity as number,
          color: "rgba(59,130,246,0.5)",
        })),
    [data],
  );

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
      crosshair: {
        mode: 1,
      },
      rightPriceScale: {
        borderColor: "#e2e8f0",
      },
      timeScale: {
        borderColor: "#e2e8f0",
        timeVisible: true,
      },
      handleScroll: true,
      handleScale: true,
    });

    const priceSeries = chart.addLineSeries({ color: "#2563eb", lineWidth: 2 });
    priceSeries.setData(lineData);

    const qtySeries = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "",
    });
    qtySeries.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    qtySeries.setData(volumeData);

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
  }, [lineData, volumeData, height]);

  return <div ref={containerRef} className="chart-container" style={{ height }} />;
}
