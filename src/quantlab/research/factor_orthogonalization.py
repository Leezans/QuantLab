from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

from quantlab.research.factors import FactorExposure


class IdentityFactorOrthogonalizer:
    def orthogonalize(
        self,
        exposures: Sequence[FactorExposure],
        *,
        factor_order: Sequence[str] | None = None,
    ) -> tuple[FactorExposure, ...]:
        del factor_order
        return tuple(exposures)


class SequentialFactorOrthogonalizer:
    def __init__(self, suffix: str = "__ortho", include_intercept: bool = True) -> None:
        self._suffix = suffix
        self._include_intercept = include_intercept

    def orthogonalize(
        self,
        exposures: Sequence[FactorExposure],
        *,
        factor_order: Sequence[str] | None = None,
    ) -> tuple[FactorExposure, ...]:
        by_timestamp: dict[object, dict[str, dict[str, FactorExposure]]] = defaultdict(lambda: defaultdict(dict))
        for exposure in exposures:
            by_timestamp[exposure.as_of][exposure.factor_name][exposure.instrument.symbol] = exposure

        resolved_order = tuple(factor_order or sorted({exposure.factor_name for exposure in exposures}))
        output: list[FactorExposure] = []
        for as_of, by_factor in sorted(by_timestamp.items(), key=lambda item: item[0]):
            orthogonalized_values: dict[str, dict[str, float]] = {}
            for factor_name in resolved_order:
                symbol_map = by_factor.get(factor_name, {})
                if not symbol_map:
                    continue
                if not orthogonalized_values:
                    residual_lookup = {symbol: exposure.value for symbol, exposure in symbol_map.items()}
                else:
                    prior_names = list(orthogonalized_values)
                    common_symbols = [
                        symbol
                        for symbol in sorted(symbol_map)
                        if all(symbol in orthogonalized_values[prior_name] for prior_name in prior_names)
                    ]
                    if len(common_symbols) <= len(prior_names):
                        residual_lookup = {symbol: exposure.value for symbol, exposure in symbol_map.items()}
                    else:
                        design = [
                            _row(
                                [orthogonalized_values[prior_name][symbol] for prior_name in prior_names],
                                include_intercept=self._include_intercept,
                            )
                            for symbol in common_symbols
                        ]
                        response = [symbol_map[symbol].value for symbol in common_symbols]
                        coefficients = _least_squares(design, response)
                        residual_lookup = {symbol: exposure.value for symbol, exposure in symbol_map.items()}
                        for symbol, row, observed in zip(common_symbols, design, response, strict=True):
                            predicted = sum(value * coefficient for value, coefficient in zip(row, coefficients, strict=True))
                            residual_lookup[symbol] = observed - predicted
                orthogonalized_values[factor_name] = residual_lookup
                output.extend(
                    FactorExposure(
                        factor_name=f"{factor_name}{self._suffix}",
                        as_of=as_of,
                        instrument=exposure.instrument,
                        value=residual_lookup[symbol],
                        feature_name=exposure.feature_name,
                        normalization=exposure.normalization,
                    )
                    for symbol, exposure in sorted(symbol_map.items())
                )
        return tuple(sorted(output, key=lambda exposure: (exposure.factor_name, exposure.as_of, exposure.instrument.symbol)))


def _row(values: Sequence[float], *, include_intercept: bool) -> list[float]:
    if include_intercept:
        return [1.0, *values]
    return list(values)


def _least_squares(design: Sequence[Sequence[float]], response: Sequence[float]) -> list[float]:
    columns = len(design[0])
    gram = [[0.0 for _ in range(columns)] for _ in range(columns)]
    rhs = [0.0 for _ in range(columns)]
    for row, target in zip(design, response, strict=True):
        for left in range(columns):
            rhs[left] += row[left] * target
            for right in range(columns):
                gram[left][right] += row[left] * row[right]
    ridge = 1e-9
    for diagonal in range(columns):
        gram[diagonal][diagonal] += ridge
    return _solve_linear_system(gram, rhs)


def _solve_linear_system(matrix: Sequence[Sequence[float]], vector: Sequence[float]) -> list[float]:
    size = len(vector)
    augmented = [list(matrix[row]) + [float(vector[row])] for row in range(size)]
    for pivot in range(size):
        best_row = max(range(pivot, size), key=lambda row: abs(augmented[row][pivot]))
        augmented[pivot], augmented[best_row] = augmented[best_row], augmented[pivot]
        pivot_value = augmented[pivot][pivot]
        if abs(pivot_value) < 1e-12:
            return [0.0 for _ in range(size)]
        for column in range(pivot, size + 1):
            augmented[pivot][column] /= pivot_value
        for row in range(size):
            if row == pivot:
                continue
            scale = augmented[row][pivot]
            for column in range(pivot, size + 1):
                augmented[row][column] -= scale * augmented[pivot][column]
    return [augmented[row][-1] for row in range(size)]
