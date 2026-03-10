from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from math import isfinite
from random import Random
from typing import Callable


@dataclass(frozen=True, slots=True)
class DimensionVector:
    price: int = 0
    volume: int = 0
    count: int = 0

    def __add__(self, other: "DimensionVector") -> "DimensionVector":
        return DimensionVector(
            price=self.price + other.price,
            volume=self.volume + other.volume,
            count=self.count + other.count,
        )

    def __sub__(self, other: "DimensionVector") -> "DimensionVector":
        return DimensionVector(
            price=self.price - other.price,
            volume=self.volume - other.volume,
            count=self.count - other.count,
        )

    @classmethod
    def dimensionless(cls) -> "DimensionVector":
        return cls()

    def render(self) -> str:
        if self == self.dimensionless():
            return "dimensionless"
        return f"p{self.price}:v{self.volume}:c{self.count}"


@dataclass(frozen=True, slots=True)
class FeatureSignature:
    name: str
    dimension: DimensionVector = DimensionVector.dimensionless()


@dataclass(frozen=True, slots=True)
class OperatorSpec:
    name: str
    arity: int
    func: Callable[..., float]
    symbol: str | None = None

    def format(self, children: Sequence[str]) -> str:
        if self.arity == 1:
            if self.symbol:
                return f"{self.symbol}({children[0]})"
            return f"{self.name}({children[0]})"
        if self.symbol:
            return f"({children[0]} {self.symbol} {children[1]})"
        return f"{self.name}({children[0]}, {children[1]})"


def _safe_div(left: float, right: float) -> float:
    if abs(right) <= 1e-12:
        return 0.0
    return left / right


def _signed_abs(value: float) -> float:
    return abs(value)


DEFAULT_UNARY_OPERATORS = (
    OperatorSpec("neg", 1, lambda value: -value, "-"),
    OperatorSpec("abs", 1, _signed_abs),
)


DEFAULT_BINARY_OPERATORS = (
    OperatorSpec("add", 2, lambda left, right: left + right, "+"),
    OperatorSpec("sub", 2, lambda left, right: left - right, "-"),
    OperatorSpec("mul", 2, lambda left, right: left * right, "*"),
    OperatorSpec("div", 2, _safe_div, "/"),
    OperatorSpec("max", 2, max),
    OperatorSpec("min", 2, min),
)


@dataclass(frozen=True, slots=True)
class FormulaNode:
    kind: str
    dimension: DimensionVector
    feature_name: str | None = None
    constant: float | None = None
    operator: OperatorSpec | None = None
    children: tuple["FormulaNode", ...] = ()

    def evaluate(self, values: dict[str, float]) -> float:
        try:
            if self.kind == "feature":
                return float(values.get(str(self.feature_name), 0.0))
            if self.kind == "constant":
                return float(self.constant or 0.0)
            if self.operator is None:
                return 0.0
            child_values = [child.evaluate(values) for child in self.children]
            result = float(self.operator.func(*child_values))
            return result if isfinite(result) else 0.0
        except Exception:
            return 0.0

    def render(self) -> str:
        if self.kind == "feature":
            return str(self.feature_name)
        if self.kind == "constant":
            return f"{float(self.constant or 0.0):.4f}"
        if self.operator is None:
            return "0.0"
        return self.operator.format([child.render() for child in self.children])

    def size(self) -> int:
        return 1 + sum(child.size() for child in self.children)

    def depth(self) -> int:
        return 1 + max((child.depth() for child in self.children), default=0)


def feature_node(signature: FeatureSignature) -> FormulaNode:
    return FormulaNode(kind="feature", dimension=signature.dimension, feature_name=signature.name)


def constant_node(value: float, dimension: DimensionVector | None = None) -> FormulaNode:
    return FormulaNode(kind="constant", dimension=dimension or DimensionVector.dimensionless(), constant=float(value))


def unary_node(operator: OperatorSpec, child: FormulaNode) -> FormulaNode:
    return FormulaNode(kind="unary", dimension=child.dimension, operator=operator, children=(child,))


def binary_node(operator: OperatorSpec, left: FormulaNode, right: FormulaNode) -> FormulaNode:
    if operator.name in {"add", "sub", "max", "min"}:
        dimension = left.dimension if left.dimension == right.dimension else DimensionVector.dimensionless()
    elif operator.name == "mul":
        dimension = left.dimension + right.dimension
    elif operator.name == "div":
        dimension = left.dimension - right.dimension
    else:
        dimension = DimensionVector.dimensionless()
    return FormulaNode(kind="binary", dimension=dimension, operator=operator, children=(left, right))


def infer_feature_signatures(
    feature_names: Iterable[str],
    dimensions: dict[str, DimensionVector] | None = None,
) -> tuple[FeatureSignature, ...]:
    resolved_dimensions = dimensions or {}
    return tuple(
        FeatureSignature(name=name, dimension=resolved_dimensions.get(name, DimensionVector.dimensionless()))
        for name in sorted(set(feature_names))
    )


def random_formula_tree(
    rng: Random,
    feature_signatures: Sequence[FeatureSignature],
    *,
    max_depth: int,
    unary_operators: Sequence[OperatorSpec] = DEFAULT_UNARY_OPERATORS,
    binary_operators: Sequence[OperatorSpec] = DEFAULT_BINARY_OPERATORS,
    constant_range: tuple[float, float] = (-2.0, 2.0),
    grow: bool = True,
) -> FormulaNode:
    if max_depth <= 1 or (grow and rng.random() < 0.35):
        if feature_signatures and rng.random() < 0.8:
            return feature_node(rng.choice(tuple(feature_signatures)))
        low, high = constant_range
        return constant_node(rng.uniform(low, high))
    use_unary = bool(unary_operators) and rng.random() < 0.3
    if use_unary:
        operator = rng.choice(tuple(unary_operators))
        return unary_node(
            operator,
            random_formula_tree(
                rng,
                feature_signatures,
                max_depth=max_depth - 1,
                unary_operators=unary_operators,
                binary_operators=binary_operators,
                constant_range=constant_range,
                grow=grow,
            ),
        )
    operator = rng.choice(tuple(binary_operators))
    return binary_node(
        operator,
        random_formula_tree(
            rng,
            feature_signatures,
            max_depth=max_depth - 1,
            unary_operators=unary_operators,
            binary_operators=binary_operators,
            constant_range=constant_range,
            grow=grow,
        ),
        random_formula_tree(
            rng,
            feature_signatures,
            max_depth=max_depth - 1,
            unary_operators=unary_operators,
            binary_operators=binary_operators,
            constant_range=constant_range,
            grow=grow,
        ),
    )


def iter_paths(node: FormulaNode, prefix: tuple[int, ...] = ()) -> tuple[tuple[int, ...], ...]:
    paths = [prefix]
    for index, child in enumerate(node.children):
        paths.extend(iter_paths(child, prefix + (index,)))
    return tuple(paths)


def subtree_at(node: FormulaNode, path: Sequence[int]) -> FormulaNode:
    current = node
    for index in path:
        current = current.children[int(index)]
    return current


def replace_subtree(node: FormulaNode, path: Sequence[int], replacement: FormulaNode) -> FormulaNode:
    if not path:
        return replacement
    index = int(path[0])
    updated_children = list(node.children)
    updated_children[index] = replace_subtree(updated_children[index], path[1:], replacement)
    if node.kind == "unary" and node.operator is not None:
        return unary_node(node.operator, updated_children[0])
    if node.kind == "binary" and node.operator is not None:
        return binary_node(node.operator, updated_children[0], updated_children[1])
    return node
