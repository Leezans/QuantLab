import { Alert, Card, Col, Divider, Row, Space, Statistic, Table, Tabs, Typography } from "antd";
import axios from "axios";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { KlineChart, type VisibleTimeRange } from "@/components/charts/KlineChart";
import { TradesChart } from "@/components/charts/TradesChart";
import { VolumeProfileChart } from "@/components/charts/VolumeProfileChart";
import { MarketDataForm, type MarketDataFormValues } from "@/components/forms/MarketDataForm";
import { RequestState } from "@/components/feedback/RequestState";
import {
  fetchKlines,
  fetchSymbols,
  fetchTrades,
  fetchVolumeProfile,
} from "@/services/api/marketDataApi";
import type {
  KlinePointDTO,
  KlinesResponseDTO,
  TradePointDTO,
  TradesResponseDTO,
  VolumeProfileResponseDTO,
} from "@/types/dto";

const PROFILE_SCAN_ROWS = 200_000;

export function CryptoLabPage() {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [symbolsError, setSymbolsError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    fetchSymbols()
      .then((data) => {
        if (mounted) {
          setSymbols(data);
        }
      })
      .catch((err: unknown) => {
        if (!mounted) {
          return;
        }
        setSymbolsError(err instanceof Error ? err.message : "Failed to load symbols");
      });

    return () => {
      mounted = false;
    };
  }, []);

  return (
    <Card>
      <Typography.Title level={3}>CryptoLab</Typography.Title>
      {symbolsError ? <Alert type="warning" showIcon message={symbolsError} /> : null}

      <Tabs
        items={[
          {
            key: "data",
            label: "Data",
            children: <CryptoDataTab symbols={symbols} />,
          },
          {
            key: "factors",
            label: "Factors",
            children: (
              <Typography.Paragraph>
                Factors module scaffolded. Next step: factor list, parameter forms, and async task panel.
              </Typography.Paragraph>
            ),
          },
          {
            key: "explorer",
            label: "Explorer",
            children: (
              <Typography.Paragraph>
                Explorer module scaffolded. Next step: universe filters, saved views, and linked charts.
              </Typography.Paragraph>
            ),
          },
        ]}
      />
    </Card>
  );
}

function CryptoDataTab({ symbols }: { symbols: string[] }) {
  return (
    <Tabs
      items={[
        {
          key: "klines",
          label: "Klines",
          children: <KlinesPanel symbols={symbols} />,
        },
        {
          key: "trades",
          label: "Trades",
          children: <TradesPanel symbols={symbols} />,
        },
      ]}
    />
  );
}

function KlinesPanel({ symbols }: { symbols: string[] }) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<KlinesResponseDTO | null>(null);
  const [profile, setProfile] = useState<VolumeProfileResponseDTO | null>(null);
  const [query, setQuery] = useState<MarketDataFormValues | null>(null);
  const [visibleRange, setVisibleRange] = useState<VisibleTimeRange | null>(null);
  const profileRequestIdRef = useRef(0);
  const combinedErrors = useMemo(
    () => [...new Set([...(result?.errors ?? []), ...(profile?.errors ?? [])])],
    [profile?.errors, result?.errors],
  );

  const onVisibleTimeRangeChange = useCallback((next: VisibleTimeRange | null) => {
    setVisibleRange((prev) => {
      if (next === null && prev === null) {
        return prev;
      }
      if (next === null) {
        return null;
      }
      const normalized = {
        from: Math.floor(next.from),
        to: Math.ceil(next.to),
      };
      if (prev && prev.from === normalized.from && prev.to === normalized.to) {
        return prev;
      }
      return normalized;
    });
  }, []);

  const onSubmit = async (values: MarketDataFormValues) => {
    profileRequestIdRef.current += 1;
    setLoading(true);
    setError(null);
    setResult(null);
    setProfile(null);
    setQuery(null);
    setVisibleRange(null);
    try {
      const klinesData = await fetchKlines({
        symbol: values.symbol,
        market: values.market,
        style: values.style,
        start: values.start,
        end: values.end,
        interval: values.interval,
        preview_rows: values.preview_rows,
      });
      setResult(klinesData);
      setQuery(values);
    } catch (err: unknown) {
      setError(formatApiError(err, "Failed to load klines"));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!result || !query) {
      return;
    }

    const requestId = profileRequestIdRef.current + 1;
    profileRequestIdRef.current = requestId;
    const timer = window.setTimeout(() => {
      fetchVolumeProfile({
        symbol: query.symbol,
        market: query.market,
        style: query.style,
        start: query.start,
        end: query.end,
        bins: query.bins,
        volume_type: query.volume_type,
        normalize: query.normalize,
        preview_rows: Math.max(query.preview_rows, PROFILE_SCAN_ROWS),
        start_ts: visibleRange?.from,
        end_ts: visibleRange?.to,
      })
        .then((profileData) => {
          if (profileRequestIdRef.current !== requestId) {
            return;
          }
          setProfile(profileData);
          setError(null);
        })
        .catch((err: unknown) => {
          if (profileRequestIdRef.current !== requestId) {
            return;
          }
          setError(formatApiError(err, "Failed to load volume profile"));
        });
    }, 280);

    return () => {
      window.clearTimeout(timer);
    };
  }, [query, result, visibleRange]);

  return (
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      <MarketDataForm mode="klines" symbols={symbols} loading={loading} onSubmit={onSubmit} />
      <RequestState loading={loading} error={error} />
      {result ? <DataMetrics source={result.source} rowCount={result.row_count} stats={result.stats} /> : null}
      {result ? (
        <KlineChart
          data={result.preview as KlinePointDTO[]}
          volumeProfile={profile?.profile ?? []}
          maPeriod={20}
          emaPeriod={50}
          onVisibleTimeRangeChange={onVisibleTimeRangeChange}
        />
      ) : null}
      {profile ? <VolumeProfileChart data={profile.profile} /> : null}
      {result ? <PathList paths={result.parquet_paths} /> : null}
      {combinedErrors.length ? <ErrorList errors={combinedErrors} /> : null}
      {result ? <KlinesPreviewTable data={result.preview} /> : null}
    </Space>
  );
}

function TradesPanel({ symbols }: { symbols: string[] }) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<TradesResponseDTO | null>(null);
  const [profile, setProfile] = useState<VolumeProfileResponseDTO | null>(null);
  const combinedErrors = useMemo(
    () => [...new Set([...(result?.errors ?? []), ...(profile?.errors ?? [])])],
    [profile?.errors, result?.errors],
  );

  const onSubmit = async (values: MarketDataFormValues) => {
    setLoading(true);
    setError(null);
    setResult(null);
    setProfile(null);
    try {
      const [tradesData, profileData] = await Promise.all([
        fetchTrades({
          symbol: values.symbol,
          market: values.market,
          style: values.style,
          start: values.start,
          end: values.end,
          preview_rows: values.preview_rows,
        }),
        fetchVolumeProfile({
          symbol: values.symbol,
          market: values.market,
          style: values.style,
          start: values.start,
          end: values.end,
          bins: values.bins,
          volume_type: values.volume_type,
          normalize: values.normalize,
          preview_rows: Math.max(values.preview_rows, PROFILE_SCAN_ROWS),
        }),
      ]);

      setResult(tradesData);
      setProfile(profileData);
    } catch (err: unknown) {
      setError(formatApiError(err, "Failed to load trades"));
    } finally {
      setLoading(false);
    }
  };

  return (
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      <MarketDataForm mode="trades" symbols={symbols} loading={loading} onSubmit={onSubmit} />
      <RequestState loading={loading} error={error} />
      {result ? <DataMetrics source={result.source} rowCount={result.row_count} stats={result.stats} /> : null}
      {result ? <TradesChart data={result.preview as TradePointDTO[]} /> : null}
      {profile ? <VolumeProfileChart data={profile.profile} /> : null}
      {result ? <PathList paths={result.parquet_paths} /> : null}
      {combinedErrors.length ? <ErrorList errors={combinedErrors} /> : null}
      {result ? <TradesPreviewTable data={result.preview} /> : null}
    </Space>
  );
}

function DataMetrics({
  source,
  rowCount,
  stats,
}: {
  source: string;
  rowCount: number;
  stats: { total_days: number; ok: number; skipped: number; failed: number };
}) {
  return (
    <>
      <Divider style={{ margin: "8px 0" }} />
      <Row gutter={16}>
        <Col span={4}>
          <Statistic title="Source" value={source} />
        </Col>
        <Col span={4}>
          <Statistic title="Rows" value={rowCount} />
        </Col>
        <Col span={4}>
          <Statistic title="Total Days" value={stats.total_days} />
        </Col>
        <Col span={4}>
          <Statistic title="OK" value={stats.ok} />
        </Col>
        <Col span={4}>
          <Statistic title="Skipped" value={stats.skipped} />
        </Col>
        <Col span={4}>
          <Statistic title="Failed" value={stats.failed} />
        </Col>
      </Row>
    </>
  );
}

function PathList({ paths }: { paths: string[] }) {
  if (!paths.length) {
    return null;
  }

  return (
    <Card size="small" title="Parquet Paths">
      <Space direction="vertical" style={{ width: "100%" }}>
        {paths.map((path) => (
          <Typography.Text key={path} copyable>
            {path}
          </Typography.Text>
        ))}
      </Space>
    </Card>
  );
}

function ErrorList({ errors }: { errors: string[] }) {
  return (
    <Card size="small" title="Errors">
      <Space direction="vertical" style={{ width: "100%" }}>
        {errors.map((item) => (
          <Alert key={item} message={item} type="error" showIcon />
        ))}
      </Space>
    </Card>
  );
}

function KlinesPreviewTable({ data }: { data: KlinePointDTO[] }) {
  const columns = useMemo(
    () => [
      { title: "Time", dataIndex: "time", key: "time" },
      { title: "Open", dataIndex: "open", key: "open" },
      { title: "High", dataIndex: "high", key: "high" },
      { title: "Low", dataIndex: "low", key: "low" },
      { title: "Close", dataIndex: "close", key: "close" },
      { title: "Volume", dataIndex: "volume", key: "volume" },
    ],
    [],
  );

  return (
    <Card size="small" title="Klines Preview">
      <Table<KlinePointDTO>
        size="small"
        rowKey={(record) => `${record.time}`}
        columns={columns}
        dataSource={data}
        pagination={{ pageSize: 20 }}
      />
    </Card>
  );
}

function TradesPreviewTable({ data }: { data: TradePointDTO[] }) {
  const columns = useMemo(
    () => [
      { title: "Time", dataIndex: "time", key: "time" },
      { title: "Price", dataIndex: "price", key: "price" },
      { title: "Qty", dataIndex: "quantity", key: "quantity" },
      { title: "Quote Qty", dataIndex: "quote_quantity", key: "quote_quantity" },
    ],
    [],
  );

  return (
    <Card size="small" title="Trades Preview">
      <Table<TradePointDTO>
        size="small"
        rowKey={(record, index) => `${record.time}-${index}`}
        columns={columns}
        dataSource={data}
        pagination={{ pageSize: 20 }}
      />
    </Card>
  );
}

function formatApiError(err: unknown, fallback: string): string {
  if (axios.isAxiosError(err)) {
    const detail = err.response?.data?.detail;
    if (typeof detail === "string" && detail.trim()) {
      return detail;
    }
    return err.message;
  }
  if (err instanceof Error) {
    return err.message;
  }
  return fallback;
}
