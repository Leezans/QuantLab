import { Layout, Menu, Typography } from "antd";
import { useMemo } from "react";
import { Outlet, useLocation, useNavigate } from "react-router-dom";

const { Header, Content } = Layout;

export function AppShell() {
  const location = useLocation();
  const navigate = useNavigate();

  const selectedKeys = useMemo(() => {
    if (location.pathname.startsWith("/labs/stocks")) {
      return ["stocks"];
    }
    if (location.pathname.startsWith("/labs/futures")) {
      return ["futures"];
    }
    return ["crypto"];
  }, [location.pathname]);

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Header className="app-header">
        <div className="app-brand">
          <Typography.Title level={4} style={{ margin: 0, color: "#fff" }}>
            QuantLab WebUI
          </Typography.Title>
        </div>
        <Menu
          theme="dark"
          mode="horizontal"
          selectedKeys={selectedKeys}
          items={[
            { key: "crypto", label: "CryptoLab" },
            { key: "stocks", label: "StocksLab" },
            { key: "futures", label: "FuturesLab" },
          ]}
          onClick={(event) => {
            navigate(`/labs/${event.key}`);
          }}
          style={{ minWidth: 420, flex: 1 }}
        />
      </Header>
      <Content className="app-content">
        <Outlet />
      </Content>
    </Layout>
  );
}
