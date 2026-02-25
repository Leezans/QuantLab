import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ColorType,
  createChart,
  type CandlestickData,
  type HistogramData,
  type ISeriesApi,
  type LineData,
  type Time,
} from "lightweight-charts";

import type { KlinePointDTO, VolumeProfileBinDTO } from "@/types/dto";
import { buildEMA, buildMA } from "@/utils/indicators";

export interface VisibleTimeRange {
  from: number;
  to: number;
}

interface KlineChartProps {
  data: KlinePointDTO[];
  volumeProfile?: VolumeProfileBinDTO[];
  maPeriod?: number;
  emaPeriod?: number;
  height?: number;
  vpWidth?: number;
  onVisibleTimeRangeChange?: (range: VisibleTimeRange | null) => void;
}

interface ProfileBar {
  top: number;
  height: number;
  widthRatio: number;
  volume: number;
}

function resolveContainerWidth(container: HTMLDivElement): number {
  const width = container.clientWidth;
  if (width > 0) {
    return width;
  }
  const parentWidth = container.parentElement?.clientWidth ?? 0;
  return Math.max(parentWidth, 320);
}

function toUnixSeconds(time: Time): number | null {
  if (typeof time === "number") {
    return Number.isFinite(time) ? time : null;
  }
  if (typeof time === "string") {
    const parsed = Date.parse(time.includes("T") ? time : `${time}T00:00:00Z`);
    return Number.isFinite(parsed) ? parsed / 1000 : null;
  }
  if (typeof time === "object" && time !== null) {
    const candidate = time as { year?: unknown; month?: unknown; day?: unknown };
    if (
      typeof candidate.year === "number" &&
      typeof candidate.month === "number" &&
      typeof candidate.day === "number"
    ) {
      const ms = Date.UTC(candidate.year, candidate.month - 1, candidate.day);
      return ms / 1000;
    }
  }
  return null;
}

export function KlineChart({
  data,
  volumeProfile = [],
  maPeriod = 20,
  emaPeriod = 50,
  height = 420,
  vpWidth = 160,
  onVisibleTimeRangeChange,
}: KlineChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const rafRef = useRef<number | null>(null);
  const [profileBars, setProfileBars] = useState<ProfileBar[]>([]);

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

  const recomputeProfileBars = useCallback(() => {
    const candleSeries = candleSeriesRef.current;
    if (!candleSeries || volumeProfile.length === 0) {
      setProfileBars([]);
      return;
    }

    const maxVolume = volumeProfile.reduce((acc, item) => Math.max(acc, item.volume), 0);
    if (maxVolume <= 0) {
      setProfileBars([]);
      return;
    }

    const points = volumeProfile
      .map((item) => {
        const y = candleSeries.priceToCoordinate(item.price);
        if (y === null) {
          return null;
        }
        const yNum = Number(y);
        if (!Number.isFinite(yNum)) {
          return null;
        }
        return { y: yNum, volume: item.volume };
      })
      .filter((item): item is { y: number; volume: number } => item !== null)
      .sort((a, b) => a.y - b.y);

    if (points.length === 0) {
      setProfileBars([]);
      return;
    }

    const chartHeight = containerRef.current?.clientHeight ?? height;
    const bars = points.map((point, index) => {
      const prev = points[index - 1];
      const next = points[index + 1];
      const prevGap = prev ? Math.abs(point.y - prev.y) : next ? Math.abs(next.y - point.y) : 8;
      const nextGap = next ? Math.abs(next.y - point.y) : prev ? Math.abs(point.y - prev.y) : 8;
      const barHeight = Math.max(2, Math.min(28, (prevGap + nextGap) / 2));
      const top = Math.max(0, Math.min(chartHeight - barHeight, point.y - barHeight / 2));
      const widthRatio = Math.max(0, Math.min(1, point.volume / maxVolume));
      return {
        top,
        height: barHeight,
        widthRatio,
        volume: point.volume,
      };
    });

    setProfileBars(bars);
  }, [height, volumeProfile]);

  const scheduleProfileRecompute = useCallback(() => {
    if (rafRef.current !== null) {
      window.cancelAnimationFrame(rafRef.current);
    }
    rafRef.current = window.requestAnimationFrame(() => {
      rafRef.current = null;
      recomputeProfileBars();
    });
  }, [recomputeProfileBars]);

  useEffect(() => {
    return () => {
      if (rafRef.current !== null) {
        window.cancelAnimationFrame(rafRef.current);
        rafRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }
    const container = containerRef.current;
    const chart = createChart(container, {
      width: resolveContainerWidth(container),
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
    candleSeriesRef.current = candleSeries;

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

    const emitVisibleRange = () => {
      if (!onVisibleTimeRangeChange) {
        return;
      }
      const visible = chart.timeScale().getVisibleRange();
      if (!visible) {
        onVisibleTimeRangeChange(null);
        return;
      }
      const from = toUnixSeconds(visible.from);
      const to = toUnixSeconds(visible.to);
      if (from === null || to === null) {
        onVisibleTimeRangeChange(null);
        return;
      }
      onVisibleTimeRangeChange({
        from: Math.floor(Math.min(from, to)),
        to: Math.ceil(Math.max(from, to)),
      });
    };

    chart.timeScale().fitContent();
    scheduleProfileRecompute();
    emitVisibleRange();

    const handleTimeRangeChanged = () => {
      scheduleProfileRecompute();
      emitVisibleRange();
    };
    chart.timeScale().subscribeVisibleTimeRangeChange(handleTimeRangeChanged);
    const handleWheel = () => {
      scheduleProfileRecompute();
      emitVisibleRange();
    };
    container.addEventListener("wheel", handleWheel, { passive: true });

    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) {
        return;
      }
      chart.applyOptions({ width: entry.contentRect.width });
      scheduleProfileRecompute();
    });
    observer.observe(container);

    return () => {
      chart.timeScale().unsubscribeVisibleTimeRangeChange(handleTimeRangeChanged);
      container.removeEventListener("wheel", handleWheel);
      observer.disconnect();
      chart.remove();
      candleSeriesRef.current = null;
    };
  }, [emaData, height, maData, ohlcData, onVisibleTimeRangeChange, scheduleProfileRecompute, volumeData]);

  const showProfile = profileBars.length > 0;
  const usableVpWidth = Math.max(60, vpWidth - 8);

  return (
    <div className="chart-container" style={{ height, position: "relative", overflow: "hidden" }}>
      <div ref={containerRef} style={{ width: "100%", height: "100%" }} />
      {showProfile ? (
        <div
          style={{
            position: "absolute",
            top: 0,
            right: 0,
            bottom: 0,
            width: vpWidth,
            pointerEvents: "none",
            background:
              "linear-gradient(270deg, rgba(248,250,252,0.95) 0%, rgba(248,250,252,0.75) 60%, rgba(248,250,252,0) 100%)",
          }}
        >
          {profileBars.map((bar, index) => (
            <div
              key={`${index}-${bar.top.toFixed(2)}`}
              title={`Volume: ${bar.volume.toFixed(4)}`}
              style={{
                position: "absolute",
                top: bar.top,
                right: 2,
                height: bar.height,
                width: Math.max(2, bar.widthRatio * usableVpWidth),
                background: "rgba(37,99,235,0.55)",
                borderRadius: "3px 0 0 3px",
              }}
            />
          ))}
        </div>
      ) : null}
    </div>
  );
}
