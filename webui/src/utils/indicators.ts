import type { LineData, UTCTimestamp } from "lightweight-charts";

import type { KlinePointDTO } from "@/types/dto";

function toUtcTime(seconds: number): UTCTimestamp {
  return seconds as UTCTimestamp;
}

export function buildMA(points: KlinePointDTO[], period: number): LineData[] {
  if (period <= 1 || points.length < period) {
    return [];
  }

  const output: LineData[] = [];
  let sum = 0;
  for (let i = 0; i < points.length; i += 1) {
    sum += points[i].close;
    if (i >= period) {
      sum -= points[i - period].close;
    }
    if (i >= period - 1) {
      output.push({ time: toUtcTime(points[i].time), value: sum / period });
    }
  }
  return output;
}

export function buildEMA(points: KlinePointDTO[], period: number): LineData[] {
  if (period <= 1 || points.length < period) {
    return [];
  }

  const k = 2 / (period + 1);
  let ema = points[0].close;
  const output: LineData[] = [{ time: toUtcTime(points[0].time), value: ema }];

  for (let i = 1; i < points.length; i += 1) {
    ema = points[i].close * k + ema * (1 - k);
    output.push({ time: toUtcTime(points[i].time), value: ema });
  }

  return output;
}
