import { Button, DatePicker, Form, Input, Select, Space, Switch } from "antd";
import type { Dayjs } from "dayjs";
import dayjs from "dayjs";

const { RangePicker } = DatePicker;

export interface MarketDataFormValues {
  symbol: string;
  market: "spot" | "futures";
  style: "mirror" | "hive";
  interval: "1m" | "5m" | "1h" | "1d";
  start: string;
  end: string;
  bins: number;
  volume_type: "base" | "quote";
  normalize: boolean;
  preview_rows: number;
}

interface FormInnerValues {
  symbol: string;
  market: "spot" | "futures";
  style: "mirror" | "hive";
  interval: "1m" | "5m" | "1h" | "1d";
  range: [Dayjs, Dayjs];
  bins: number;
  volume_type: "base" | "quote";
  normalize: boolean;
  preview_rows: number;
}

interface MarketDataFormProps {
  mode: "klines" | "trades";
  symbols: string[];
  loading: boolean;
  onSubmit: (values: MarketDataFormValues) => void;
}

export function MarketDataForm({ mode, symbols, loading, onSubmit }: MarketDataFormProps) {
  const [form] = Form.useForm<FormInnerValues>();

  const defaultSymbol = symbols[0] ?? "BTCUSDT";
  const defaultEnd = dayjs().subtract(1, "day");
  const defaultStart = defaultEnd.subtract(7, "day");

  return (
    <Form<FormInnerValues>
      layout="inline"
      form={form}
      initialValues={{
        symbol: defaultSymbol,
        market: "spot",
        style: "mirror",
        interval: "1h",
        range: [defaultStart, defaultEnd],
        bins: 80,
        volume_type: "base",
        normalize: false,
        preview_rows: mode === "trades" ? 2000 : 3000,
      }}
      onFinish={(values) => {
        onSubmit({
          symbol: values.symbol.trim().toUpperCase(),
          market: values.market,
          style: values.style,
          interval: values.interval,
          start: values.range[0].format("YYYY-MM-DD"),
          end: values.range[1].format("YYYY-MM-DD"),
          bins: values.bins,
          volume_type: values.volume_type,
          normalize: values.normalize,
          preview_rows: values.preview_rows,
        });
      }}
    >
      <Form.Item name="symbol" label="Symbol">
        <Select
          showSearch
          style={{ width: 140 }}
          options={symbols.length ? symbols.map((s) => ({ label: s, value: s })) : undefined}
          popupRender={(menu) => (
            <>
              {menu}
              <div style={{ padding: 8 }}>
                <Input
                  placeholder="Custom symbol"
                  onPressEnter={(event) => {
                    const value = (event.target as HTMLInputElement).value.trim().toUpperCase();
                    if (!value) {
                      return;
                    }
                    form.setFieldValue("symbol", value);
                  }}
                />
              </div>
            </>
          )}
        />
      </Form.Item>

      <Form.Item name="market" label="Market">
        <Select style={{ width: 100 }} options={[{ value: "spot" }, { value: "futures" }]} />
      </Form.Item>

      <Form.Item name="style" label="Layout">
        <Select style={{ width: 100 }} options={[{ value: "mirror" }, { value: "hive" }]} />
      </Form.Item>

      {mode === "klines" ? (
        <Form.Item name="interval" label="Interval">
          <Select
            style={{ width: 90 }}
            options={[{ value: "1m" }, { value: "5m" }, { value: "1h" }, { value: "1d" }]}
          />
        </Form.Item>
      ) : null}

      <Form.Item name="range" label="Date Range">
        <RangePicker allowClear={false} />
      </Form.Item>

      <Form.Item name="preview_rows" label="Preview Rows">
        <Input type="number" min={100} max={50000} style={{ width: 110 }} />
      </Form.Item>

      <Space>
        <Form.Item name="bins" label="Bins">
          <Input type="number" min={10} max={300} style={{ width: 90 }} />
        </Form.Item>
        <Form.Item name="volume_type" label="Volume">
          <Select style={{ width: 100 }} options={[{ value: "base" }, { value: "quote" }]} />
        </Form.Item>
        <Form.Item name="normalize" label="Normalize" valuePropName="checked">
          <Switch />
        </Form.Item>
      </Space>

      <Form.Item>
        <Button type="primary" htmlType="submit" loading={loading}>
          Load
        </Button>
      </Form.Item>
    </Form>
  );
}
