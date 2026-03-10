from __future__ import annotations

from quantlab.cli import build_factor_research_bars, build_factor_workflow, format_factor_research_result


def main() -> None:
    workflow = build_factor_workflow("config/base.toml", artifact_name="factor_demo")
    result = workflow.run(
        build_factor_research_bars(),
        version="demo-20240101",
        metadata={
            "source": "synthetic_demo",
            "universe": "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT",
        },
    )
    print(format_factor_research_result(result, universe="BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT"))


if __name__ == "__main__":
    main()
