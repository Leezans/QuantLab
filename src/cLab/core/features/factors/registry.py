# src/cLab/core/features/factors/registry.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence


Series = Sequence[float]
SeriesDict = Mapping[str, Series]
FactorComputeFn = Callable[[SeriesDict, Mapping[str, object]], List[float]]


@dataclass(frozen=True, slots=True)  # slots=True 限制属性集合并降低内存开销
class FactorSpec:  # 定义“因子规格”数据结构：用于描述一个因子的元信息与计算函数
    name: str  # 因子唯一名称（注册键），例如 rolling_mean_return
    compute: FactorComputeFn  # 因子计算函数，签名应为 (data, params) -> List[float]
    required_fields: tuple[str, ...]  # 该因子依赖的必需字段集合，例如 ("close",)
    param_defaults: Dict[str, object] = field(default_factory=dict)  # 因子默认参数字典；用 default_factory 避免可变默认值陷阱
    description: str = ""  # 因子的人类可读描述信息，默认空字符串

    def __post_init__(self) -> None:  # dataclass 初始化后自动执行的校验钩子，用于保证对象处于合法状态
        if not self.name.strip():  # 去除首尾空白后若名称为空，说明传入了空名或全空格名
            raise ValueError("FactorSpec.name must be non-empty")  # 抛出异常，阻止无效因子规格进入注册表
        if not callable(self.compute):  # 校验 compute 是否为可调用对象（函数/实现 __call__ 的对象）
            raise ValueError("FactorSpec.compute must be callable")  # 若不可调用则报错，避免后续执行失败
        if not self.required_fields:  # 校验必需字段列表是否非空
            raise ValueError("FactorSpec.required_fields must be non-empty")  # 若为空则报错，避免定义缺乏输入约束的因子


class FactorRegistry:  # 因子注册中心：负责存储、查询、枚举与执行所有已注册因子
    def __init__(self) -> None:  # 构造函数：初始化一个空的内部注册表
        self._specs: MutableMapping[str, FactorSpec] = {}  # 内部字典：键是因子名，值是 FactorSpec

    def register(self, spec: FactorSpec) -> None:  # 注册一个因子规格到注册表
        key = spec.name  # 取因子名作为唯一键
        if key in self._specs:  # 若同名键已存在，说明发生重复注册
            raise ValueError(f"Factor '{key}' is already registered")  # 抛异常阻止覆盖，保证名称唯一
        self._specs[key] = spec  # 将因子规格写入注册表

    def get(self, name: str) -> FactorSpec:  # 按名称获取单个因子规格
        try:  # 使用 try/except 将底层 KeyError 转换为更友好的报错信息
            return self._specs[name]  # 命中时直接返回对应的 FactorSpec
        except KeyError as e:  # 未命中时捕获原始 KeyError
            raise KeyError(f"Unknown factor '{name}'") from e  # 抛出带上下文的新 KeyError

    def list(self) -> List[FactorSpec]:  # 返回所有已注册因子规格（按名称排序）
        return sorted(self._specs.values(), key=lambda s: s.name)  # 对字典值按 spec.name 升序排序后返回

    def names(self) -> List[str]:  # 返回所有因子名称列表
        return [s.name for s in self.list()]  # 在有序规格列表基础上提取 name 字段

    def compute(  # 统一计算入口：按因子名执行对应因子函数
        self,  # 实例自身
        name: str,  # 待执行的因子名称
        data: SeriesDict,  # 输入数据字典（如 close/volume 等序列）
        params: Optional[Mapping[str, object]] = None,  # 运行时传入的参数字典，可为空
        *,  # 其后参数必须以关键字方式传入
        strict_fields: bool = True,  # 是否严格检查 required_fields 是否齐全
    ) -> List[float]:  # 返回因子计算结果序列
        spec = self.get(name)  # 先按名称取到对应的因子规格
        if strict_fields:  # 若开启严格字段检查
            missing = [f for f in spec.required_fields if f not in data]  # 计算缺失字段列表
            if missing:  # 若存在缺失字段
                raise KeyError(f"Factor '{name}' missing required fields: {missing}")  # 抛出错误提示缺少哪些字段

        merged: Dict[str, object] = dict(spec.param_defaults)  # 先复制默认参数，避免修改原 defaults
        if params:  # 若调用方提供了运行时参数
            merged.update(params)  # 用运行时参数覆盖默认参数（同名键以运行时为准）

        return spec.compute(data, merged)  # 调用真正的因子函数并返回结果


_global_registry = FactorRegistry()


def register_factor(  # 定义一个“装饰器工厂”函数：先接收配置，再返回真正的装饰器
    name: str,  # 因子名称，例如 "rolling_mean_return"
    *,  # 从这一行开始，后面的参数必须用关键字传参（可读性更高，也不容易传错位置）
    required_fields: Iterable[str],  # 这个因子计算时必须存在的数据列名，如 ("close",)
    param_defaults: Optional[Mapping[str, object]] = None,  # 因子参数默认值；可不传
    description: str = "",  # 因子描述信息；默认空字符串
) -> Callable[[FactorComputeFn], FactorComputeFn]:  # 返回值类型：接收一个因子函数，返回一个因子函数（标准装饰器签名）
    req = tuple(required_fields)  # 把 required_fields 统一转成 tuple，便于后续固定保存
    defaults = dict(param_defaults) if param_defaults else {}  # 把默认参数拷贝成普通 dict；若未提供则用空 dict

    def _decorator(fn: FactorComputeFn) -> FactorComputeFn:  # 真正的装饰器：参数 fn 就是被 @ 装饰的那个函数
        _global_registry.register(  # 把这个函数和它的元信息注册到“全局因子表”
            FactorSpec(  # 构造一个因子规格对象（元数据 + 计算函数）
                name=name,  # 注册名：外部通过这个名字查找并执行因子
                compute=fn,  # 实际计算逻辑函数
                required_fields=req,  # 依赖字段列表
                param_defaults=defaults,  # 默认参数
                description=description,  # 描述信息
            )
        )
        return fn  # 返回原函数本身：函数行为不变，只是多了“注册”这个副作用

    return _decorator  # 把装饰器返回给 @register_factor(...) 使用


def get_registry() -> FactorRegistry:
    return _global_registry