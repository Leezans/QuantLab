import { useEffect, useMemo, useRef } from "react";
import {
  ColorType,
  createChart,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type Time,
} from "lightweight-charts";

import type { KlinePointDTO } from "@/types/dto";
import { buildEMA, buildMA } from "@/utils/indicators";

interface KlineChartProps {
  data: KlinePointDTO[];
  maPeriod?: number;
  emaPeriod?: number;
  height?: number;
}

export function KlineChart({ data, maPeriod = 20, emaPeriod = 50, height = 420 }: KlineChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);

  const ohlcData = useMemo<CandlestickData<Time>[]>(
    () =>
      data.map((item) => ({
        time: item.time as Time,
        open: item.open,
        high: item.high,
        low: item.low,
        close: item.close,
      })),
    [data],
  );

  const volumeData = useMemo<HistogramData<Time>[]>(
    () =>
      data
        .filter((item) => item.volume !== undefined && item.volume !== null)
        .map((item) => ({
          time: item.time as Time,
          value: item.volume as number,
          color: item.close >= item.open ? "rgba(34,197,94,0.5)" : "rgba(239,68,68,0.5)",
        })),
    [data],
  );

  const maData = useMemo<LineData[]>(() => buildMA(data, maPeriod), [data, maPeriod]);
  const emaData = useMemo<LineData[]>(() => buildEMA(data, emaPeriod), [data, emaPeriod]);

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
        secondsVisible: false,
      },
      handleScroll: true,
      handleScale: true,
    });

    const candleSeries = chart.addCandlestickSeries();
    candleSeries.setData(ohlcData);

    const volumeSeries: ISeriesApi<"Histogram"> = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "",
    });
    volumeSeries.priceScale().applyOptions({ scaleMargins: { top: 0.75, bottom: 0 } });
    volumeSeries.setData(volumeData);

    if (maData.length > 0) {
      const maSeries = chart.addLineSeries({ color: "#0ea5e9", lineWidth: 2 });
      maSeries.setData(maData);
    }

    if (emaData.length > 0) {
      const emaSeries = chart.addLineSeries({ color: "#f97316", lineWidth: 2 });
      emaSeries.setData(emaData);
    }

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
  }, [ohlcData, volumeData, maData, emaData, height]);

  return <div ref={containerRef} className="chart-container" style={{ height }} />;
}
