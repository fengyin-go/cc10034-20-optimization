"""格式适配领域服务：将变更事件在不同格式间转换。

上游：change_event 产出已解析事件 → format_adapter 适配格式 (Debezium/Canal/Maxwell)
消费：scd_handler / event_filter 消费适配后的事件

标准镜像（canonical mirror）约定：
- 对变更前/后镜像的字段按“值转换”规则逐字段转换；
- 显式 null 原样保留（用于让下游执行清除），绝不当成缺字段；
- 源记录根本没有传的字段保持缺省（missing_fields 单独列出），与显式 null 区分；
- 仅当规则配置了 default: 缺省填充时才补字段，并在 default_filled 中标注；
- 事务首条事件到达时绑定规则版本，整笔事务固定同一版规则，保证重放稳定。
"""
from __future__ import annotations

import json
import zlib
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class FormatAdapterService(BaseModuleService):
    MODULE = "format_adapter"
    ENTITY = "适配规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则名称", "输入格式", "输出格式", "字段重命名", "值转换", "时间戳格式", "是否压缩"]
    REQUIRED_FIELDS = ["规则名称", "输入格式", "输出格式"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用规则": "草稿", "停用规则": "已启用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # ------------------------------------------------------------------
    # 规则版本登记
    # ------------------------------------------------------------------
    def register_rule_version(self, config: dict[str, Any]) -> dict[str, Any]:
        """登记一版不可变适配规则并切为当前启用版本（版本交接入口）。"""
        return pipeline_bus.register_adapter_rule(config)

    def get_active_rule(self, rule_name: str) -> dict[str, Any] | None:
        return pipeline_bus.get_active_adapter_rule(rule_name)

    # ------------------------------------------------------------------
    # 标准镜像
    # ------------------------------------------------------------------
    def build_standard_mirror(self, event: dict[str, Any],
                              rule_snapshot: dict[str, Any]) -> dict[str, Any]:
        """把一条变更事件按指定版本的规则产出标准镜像数据。

        标准镜像严格区分：
        - 显式 null（key 存在且值为 None）→ 保留在镜像里，下游可据此清除原值；
        - 缺字段（key 根本不在源记录里）→ 不进镜像，记入 missing_fields；
          仅当规则配置 default:<值> 时才补默认值并在 default_filled 标注。
        """
        config = rule_snapshot["config"]
        field_rename = self._parse_mapping(config.get("字段重命名", ""))
        value_convert = self._parse_mapping(config.get("值转换", ""))
        default_values = self._parse_defaults(config.get("缺省填充", ""))

        columns = list(event.get("columns") or [])
        before_image, before_missing, before_null = self._load_image(
            event.get("变更前镜像"), columns)
        after_image, after_missing, after_null = self._load_image(
            event.get("变更后镜像"), columns)

        before_std = self._convert_image(
            before_image, before_missing, before_null, value_convert, default_values)
        after_std = self._convert_image(
            after_image, after_missing, after_null, value_convert, default_values)

        for old_name, new_name in field_rename.items():
            for image in (before_std, after_std):
                if old_name in image:
                    image[new_name] = image.pop(old_name)

        tx_id = str(event.get("事务ID", ""))
        return {
            "event_id": event.get("事件ID", ""),
            "tx_id": tx_id,
            "source_table": event.get("来源表", ""),
            "operation": str(event.get("操作类型", "")).upper(),
            "event_time": event.get("事件时间", ""),
            "before": before_std,
            "after": after_std,
            # 缺字段（源记录没传）与显式 null 分开记录
            "missing_before": sorted(before_missing),
            "missing_after": sorted(after_missing),
            "null_before": sorted(before_null),
            "null_after": sorted(after_null),
            # 由缺省填充补上的字段及其默认值，供下游识别“默认值更新”
            "default_filled_before": {
                k: default_values[k] for k in before_std["_default_filled"]
            },
            "default_filled_after": {
                k: default_values[k] for k in after_std["_default_filled"]
            },
            "rule_name": rule_snapshot["rule_name"],
            "rule_version": rule_snapshot["version"],
            "raw_event": event,
            "coordinate": pipeline_bus.source_coordinate(event),
        }

    def adapt_transaction(self, events: list[dict[str, Any]],
                          rule_name: str) -> list[dict[str, Any]]:
        """适配同一事务的一批事件。

        首条事件到达时把事务绑定到规则当前启用版本，之后即使发生版本交接，
        同一事务的所有行仍按首条绑定时的版本转换。
        """
        if not events:
            return []
        tx_id = str(events[0].get("事务ID", ""))
        binding = pipeline_bus.bind_transaction_rule(tx_id, rule_name)
        snapshot = binding["snapshot"]
        return [self.build_standard_mirror(evt, snapshot) for evt in events]

    def replay_transaction(self, tx_id: str) -> list[dict[str, Any]]:
        """按事务绑定的原规则版本重放整笔事务，产出与首次一致的标准镜像。"""
        bundle = pipeline_bus.get_dispatched_transaction(tx_id)
        if bundle is None:
            raise ValueError(f"事务 {tx_id} 没有已下发档案，无法回查重放")
        snapshot = pipeline_bus.get_adapter_rule(
            bundle["rule_name"], bundle["rule_version"])
        if snapshot is None:
            raise ValueError(
                f"规则 {bundle['rule_name']} 版本 {bundle['rule_version']} 已不存在")
        return [self.build_standard_mirror(evt, snapshot) for evt in bundle["records"]]

    # ------------------------------------------------------------------
    # 镜像解析与值转换
    # ------------------------------------------------------------------
    def _load_image(self, raw: Any, columns: list[str]) -> tuple[
            dict[str, Any], set[str], set[str]]:
        """解析镜像，返回 (字段字典, 缺字段集合, 显式 null 字段集合)。

        缺字段：columns 声明了但源记录没传的字段；没有 columns 时缺字段集合为空。
        显式 null：key 存在且值就是 None。
        """
        if isinstance(raw, str):
            try:
                image = json.loads(raw) if raw.strip() else {}
            except json.JSONDecodeError:
                image = {}
        elif raw is None:
            image = {}
        elif isinstance(raw, dict):
            image = dict(raw)
        else:
            image = {}

        null_fields = {key for key, val in image.items() if val is None}
        present = set(image.keys())
        missing = {col for col in columns if col not in present}
        return image, missing, null_fields

    def _convert_image(self, image: dict[str, Any], missing: set[str],
                       null_fields: set[str], value_convert: dict[str, str],
                       default_values: dict[str, Any]) -> dict[str, Any]:
        """对镜像字段按值转换规则转换；显式 null 透传，缺字段按规则填默认值。"""
        result: dict[str, Any] = {}
        for key, val in image.items():
            if val is None:
                # 显式 null 原样保留，不做转换、不被默认值覆盖
                result[key] = None
                continue
            if key in value_convert:
                result[key] = self._apply_converter(val, value_convert[key])
            else:
                result[key] = val

        filled: list[str] = []
        for key, default_val in default_values.items():
            if key in missing and key not in result:
                # 只有源记录根本没传时才补；显式 null 不补，保证清除语义
                result[key] = default_val
                filled.append(key)
        result["_default_filled"] = filled
        return result

    def _apply_converter(self, val: Any, converter_exp: str) -> Any:
        """按单个值转换表达式转换一个非 null 值。"""
        if converter_exp == "lower":
            return str(val).lower()
        if converter_exp == "upper":
            return str(val).upper()
        if converter_exp.startswith("replace:"):
            parts = converter_exp.split(":", 2)
            if len(parts) == 3:
                return str(val).replace(parts[1], parts[2])
            return str(val)
        if converter_exp.startswith("round:"):
            digits = self._safe_int(converter_exp.split(":", 1)[1], 0)
            try:
                return round(float(val), digits)
            except (TypeError, ValueError):
                return val
        if converter_exp == "number":
            try:
                num = float(val)
                return int(num) if num.is_integer() else num
            except (TypeError, ValueError):
                return val
        return val

    def _parse_defaults(self, mapping_str: str) -> dict[str, Any]:
        """解析缺省填充配置，形如 discount:0,status:unknown。"""
        result: dict[str, Any] = {}
        for key, val in self._parse_mapping(mapping_str).items():
            result[key] = self._coerce_scalar(val)
        return result

    @staticmethod
    def _coerce_scalar(val: str) -> Any:
        text = val.strip()
        if text.lower() == "null":
            return None
        try:
            num = float(text)
            return int(num) if num.is_integer() else num
        except ValueError:
            return text

    @staticmethod
    def _safe_int(text: str, default: int) -> int:
        try:
            return int(text.strip())
        except (ValueError, AttributeError):
            return default

    # ------------------------------------------------------------------
    # 历史能力：Debezium/Canal/Maxwell 互转（保持原有接口不变）
    # ------------------------------------------------------------------
    def adapt_event(self, event: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """将变更事件按规则适配为目标格式。

        支持 Debezium ↔ Canal ↔ Maxwell 格式互转，
        包括字段重命名、值转换、时间戳格式化和压缩。
        """
        input_fmt = config.get("输入格式", "debezium").lower()
        output_fmt = config.get("输出格式", "canal").lower()
        field_rename = self._parse_mapping(config.get("字段重命名", ""))
        value_convert = self._parse_mapping(config.get("值转换", ""))
        ts_format = config.get("时间戳格式", "%Y-%m-%d %H:%M:%S")
        should_compress = config.get("是否压缩", "否") == "是"

        # Debezium -> 内部格式
        if input_fmt == "debezium":
            adapted = self._from_debezium(event)
        elif input_fmt == "canal":
            adapted = self._from_canal(event)
        elif input_fmt == "maxwell":
            adapted = self._from_maxwell(event)
        else:
            adapted = dict(event)

        # 字段重命名
        for old_name, new_name in field_rename.items():
            if old_name in adapted:
                adapted[new_name] = adapted.pop(old_name)

        # 值转换（仅转换实际出现的字段；显式 null 不转换）
        for field_name, converter_exp in value_convert.items():
            if field_name in adapted and adapted[field_name] is not None:
                val = adapted[field_name]
                adapted[field_name] = self._apply_converter(val, converter_exp)

        # 时间戳格式化
        ts = event.get("事件时间", "")
        if ts:
            try:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                adapted["formatted_timestamp"] = dt.strftime(ts_format)
            except (ValueError, TypeError):
                adapted["formatted_timestamp"] = str(ts)

        # 压缩
        if should_compress:
            payload = json.dumps(adapted, ensure_ascii=False)
            compressed = zlib.compress(payload.encode("utf-8"))
            adapted = {"_compressed": True, "_size": len(compressed), "_format": output_fmt}

        adapted["_output_format"] = output_fmt
        adapted["_adapted_at"] = datetime.now().isoformat()
        return adapted

    def _from_debezium(self, event: dict[str, Any]) -> dict[str, Any]:
        result = dict(event)
        before_raw = event.get("变更前镜像", "{}")
        after_raw = event.get("变更后镜像", "{}")
        # Debezium 的 payload.after 结构
        result["source_meta"] = {
            "connector": "mysql",
            "version": "1.9.7.Final",
            "name": event.get("来源表", ""),
            "ts_ms": int(datetime.now().timestamp() * 1000),
        }
        try:
            after = json.loads(after_raw) if isinstance(after_raw, str) else after_raw
            result["payload"] = {"before": before_raw, "after": after}
        except (json.JSONDecodeError, TypeError):
            result["payload"] = {"before": {}, "after": {}}
        return result

    def _from_canal(self, event: dict[str, Any]) -> dict[str, Any]:
        result = dict(event)
        op_map = {"insert": "INSERT", "update": "UPDATE", "delete": "DELETE"}
        dt_now = int(datetime.now().timestamp() * 1000)
        result["canal_meta"] = {
            "binlog": f"mysql-bin.{dt_now // 1000 % 1000000:06d}",
            "type": op_map.get(event.get("操作类型", "").lower(), "INSERT"),
            "es": dt_now,
            "isDdl": False,
        }
        return result

    def _from_maxwell(self, event: dict[str, Any]) -> dict[str, Any]:
        result = dict(event)
        source = event.get("来源表", "")
        result["maxwell_meta"] = {
            "database": source.split(".")[0] if "." in source else "default_db",
            "table": source.split(".")[-1] if "." in source else "default_table",
            "type": event.get("操作类型", ""),
            "ts": int(datetime.now().timestamp()),
            "xid": event.get("事务ID", ""),
            "commit": True,
        }
        return result

    def _parse_mapping(self, mapping_str: str) -> dict[str, str]:
        """解析字段重命名/值转换字符串。"""
        result: dict[str, str] = {}
        if not mapping_str:
            return result
        for pair in str(mapping_str).split(","):
            pair = pair.strip()
            if ":" in pair:
                key, val = pair.split(":", 1)
                result[key.strip()] = val.strip()
            elif "=" in pair:
                key, val = pair.split("=", 1)
                result[key.strip()] = val.strip()
        return result

    def batch_adapt(self, events: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
        """批量适配事件。"""
        adapted_list = [self.adapt_event(evt, config) for evt in events]
        compressed_count = sum(1 for a in adapted_list if a.get("_compressed"))
        return {
            "total": len(adapted_list),
            "compressed": compressed_count,
            "output_format": config.get("输出格式"),
            "adapted_at": datetime.now().isoformat(),
        }

    def compute_format_compatibility(self, source_fmt: str, target_fmt: str) -> dict[str, Any]:
        """计算两种格式的兼容性。"""
        compatibility_matrix = {
            ("debezium", "canal"): 95,
            ("canal", "debezium"): 90,
            ("debezium", "maxwell"): 85,
            ("maxwell", "debezium"): 80,
            ("canal", "maxwell"): 90,
            ("maxwell", "canal"): 90,
        }
        score = compatibility_matrix.get((source_fmt.lower(), target_fmt.lower()), 70)
        return {"source": source_fmt, "target": target_fmt, "compatibility_pct": score, "lossless": score >= 95}
