import { Alert, Spin } from "antd";

interface RequestStateProps {
  loading: boolean;
  error: string | null;
}

export function RequestState({ loading, error }: RequestStateProps) {
  if (loading) {
    return <Spin size="large" />;
  }

  if (error) {
    return <Alert type="error" message={error} showIcon />;
  }

  return null;
}
