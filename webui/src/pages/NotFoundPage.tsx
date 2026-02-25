import { Button, Result } from "antd";
import { useNavigate } from "react-router-dom";

export function NotFoundPage() {
  const navigate = useNavigate();

  return (
    <Result
      status="404"
      title="404"
      subTitle="Page not found"
      extra={
        <Button type="primary" onClick={() => navigate("/labs/crypto")}>
          Back to CryptoLab
        </Button>
      }
    />
  );
}
