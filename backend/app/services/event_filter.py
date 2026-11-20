"""事件过滤领域服务：对变更事件执行黑白名单过滤与字段级变更判定。

上游：change_event + format_adapter 产出标准镜像 → event_filter 过滤
下游：route_dispatcher 消费过滤后的事件进行路由分发
审计：audit_trail 记录过滤操作

字段级过滤约定（消费标准镜像）：
- 显式 null 且原值非 null → clear（清除），下游可以应用；
- 字段前后值不同且非 null → update（更新）；值相同 → none（无变动，保留但不下发字段）；
- 源记录没传的缺字段一律不产生任何变更，绝不与显式 null 混为清除；
- 同源坐标重复提交不再放行；已下发事务可按原规则版本回查与整笔重放。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class EventFilterService(BaseModuleService):
    MODULE = "event_filter"
    ENTITY = "过滤规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "过滤类型", "匹配表达式", "是否白名单", "优先级", "生效范围", "备注说明"]
    REQUIRED_FIELDS = ["规则名称", "过滤类型", "匹配表达式"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用规则": "草稿", "停用规则": "已启用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # ------------------------------------------------------------------
    # 标准镜像字段级过滤
    # ------------------------------------------------------------------
    def filter_standard_event(self, standard: dict[str, Any],
                              filter_config: dict[str, Any] | None = None,
                              *, replay: bool = False) -> dict[str, Any]:
        """对一条标准镜像做字段级变更过滤，决定是否放行以及下游可应用的数据。

        返回 decision：
        - emit：放行，payload 中只保留下游能够应用的清除/更新/无变动数据；
        - block：规则拦截（表/操作黑白名单）或同源坐标重复提交。
        """
        coordinate = standard.get("coordinate") or pipeline_bus.source_coordinate(
            standard.get("raw_event", standard))

        # 同源坐标重复提交不再放行（重放走回查通道，不重复下发）
        if not replay and pipeline_bus.is_coordinate_dispatched(coordinate):
            return self._decision(
                standard, "block", "duplicate_coordinate",
                [], {"reason": f"源坐标 {coordinate} 已下发，重复提交不再放行"})

        # 黑白名单规则（表名/操作类型匹配）
        if filter_config:
            blocked, reason = self._match_block_rule(standard, filter_config)
            if blocked:
                pipeline_bus.pipeline_metrics["filtered"] += 1
                return self._decision(standard, "block", "rule_blocked", [], {"reason": reason})

        changes = self._diff_fields(standard)
        clears = [c for c in changes if c["change_kind"] == "clear"]
        updates = [c for c in changes if c["change_kind"] == "update"]
        nones = [c for c in changes if c["change_kind"] == "none"]

        if standard["operation"] == "INSERT":
            overall = "insert"
        elif standard["operation"] == "DELETE":
            overall = "delete"
        else:
            overall = "change" if (clears or updates) else "none"

        payload = self._build_downstream_payload(standard, changes, overall)
        decision = self._decision(
            standard, "emit", overall, changes,
            {"payload": payload, "replay": replay})

        if not replay:
            # 放行后登记坐标，后续同源坐标重复提交直接拦截
            pipeline_bus.mark_coordinate_dispatched(coordinate)
            pipeline_bus.pipeline_metrics["total_output"] += 1
        return decision

    def filter_transaction(self, standards: list[dict[str, Any]],
                           filter_config: dict[str, Any] | None = None,
                           *, replay: bool = False) -> list[dict[str, Any]]:
        """过滤同一事务的全部标准镜像，逐行给出放行/拦截决定。"""
        return [
            self.filter_standard_event(std, filter_config, replay=replay)
            for std in standards
        ]

    def commit_transaction(self, standards: list[dict[str, Any]],
                           decisions: list[dict[str, Any]]) -> dict[str, Any]:
        """提交事务：保存原始事件与绑定规则版本，使整笔事务可按原规则回查重放。"""
        if not standards:
            raise ValueError("空事务无法提交")
        first = standards[0]
        tx_id = first["tx_id"]
        raw_records = [std["raw_event"] for std in standards]
        bundle = pipeline_bus.commit_transaction(
            tx_id, raw_records, first["rule_name"], first["rule_version"])
        pipeline_bus.write_audit(
            "事务提交", tx_id,
            (f"事务 {tx_id} 已下发：规则 {first['rule_name']} v{first['rule_version']}，"
             f"共 {len(standards)} 行，放行 "
             f"{sum(1 for d in decisions if d['decision'] == 'emit')} 行"),
            "event_filter", "已下发")
        return bundle

    def replay_transaction(self, tx_id: str,
                           filter_config: dict[str, Any] | None = None) -> dict[str, Any]:
        """按事务档案中的原规则版本整笔重放，并核对与首次下发是否一致。"""
        bundle = pipeline_bus.get_dispatched_transaction(tx_id)
        if bundle is None:
            raise ValueError(f"事务 {tx_id} 没有已下发档案，无法回查重放")
        # 延迟导入避免模块加载循环
        from app.services.format_adapter import FormatAdapterService
        adapter = FormatAdapterService()
        standards = adapter.replay_transaction(tx_id)
        decisions = self.filter_transaction(standards, filter_config, replay=True)
        emitted_payloads = [
            d["payload"] for d in decisions if d["decision"] == "emit"
        ]
        return {
            "tx_id": tx_id,
            "rule_name": bundle["rule_name"],
            "rule_version": bundle["rule_version"],
            "standards": standards,
            "decisions": decisions,
            "emitted_payloads": emitted_payloads,
        }

    # ------------------------------------------------------------------
    # 字段级差异：清除 / 更新 / 无变动；缺字段不产生变更
    # ------------------------------------------------------------------
    def _diff_fields(self, standard: dict[str, Any]) -> list[dict[str, Any]]:
        before = standard.get("before", {})
        after = standard.get("after", {})
        default_filled_after = standard.get("default_filled_after", {})
        missing_after = set(standard.get("missing_after", []))

        changes: list[dict[str, Any]] = []
        # 只比较后镜像里实际存在的字段（含显式 null）；缺字段天然不参与
        for key in after:
            if key.startswith("_"):
                continue
            new_val = after[key]
            old_present = key in before and not key.startswith("_")
            old_val = before.get(key)

            if new_val is None:
                # 显式 null：只有原值存在且非 null 才构成清除
                kind = "clear" if (old_present and old_val is not None) else "none"
            elif not old_present:
                kind = "update"
            elif old_val != new_val:
                kind = "update"
            else:
                kind = "none"

            changes.append({
                "field": key,
                "change_kind": kind,
                "old": old_val if old_present else None,
                "new": new_val,
                "old_present": old_present,
                "explicit_null": new_val is None,
                "default_filled": key in default_filled_after,
                "missing_in_source": key in missing_after,
            })
        return changes

    def _build_downstream_payload(self, standard: dict[str, Any],
                                  changes: list[dict[str, Any]],
                                  overall: str) -> dict[str, Any]:
        """组装下游可应用载荷：只保留清除、更新字段与无变动键列表。"""
        key_fields = self._extract_key_fields(standard)
        clear_fields = {c["field"]: None for c in changes if c["change_kind"] == "clear"}
        update_fields = {
            c["field"]: c["new"]
            for c in changes
            if c["change_kind"] == "update"
        }
        none_fields = [c["field"] for c in changes if c["change_kind"] == "none"]
        default_updates = {
            c["field"]: c["new"]
            for c in changes
            if c["change_kind"] == "update" and c["default_filled"]
        }
        return {
            "tx_id": standard["tx_id"],
            "event_id": standard["event_id"],
            "source_table": standard["source_table"],
            "operation": standard["operation"],
            "overall": overall,
            "key_fields": key_fields,
            # 下游真正能 apply 的数据：清除（显式 null）与更新
            "clear_fields": clear_fields,
            "update_fields": update_fields,
            # 由缺省填充触发的默认值更新，单独可见
            "default_updates": default_updates,
            # 无变动字段保留记录但不携带值，下游无需执行写入
            "unchanged_fields": none_fields,
            "rule_name": standard["rule_name"],
            "rule_version": standard["rule_version"],
        }

    @staticmethod
    def _extract_key_fields(standard: dict[str, Any]) -> dict[str, Any]:
        """从前/后镜像中提取标识键（id/order_id/代码类字段）。"""
        key_candidates = ("id", "order_id", "pk", "code")
        keys: dict[str, Any] = {}
        for source in (standard.get("after", {}), standard.get("before", {})):
            for candidate in key_candidates:
                if candidate in source and candidate not in keys:
                    keys[candidate] = source[candidate]
        return keys

    def _match_block_rule(self, standard: dict[str, Any],
                          config: dict[str, Any]) -> tuple[bool, str]:
        """按黑白名单配置判定是否拦截。返回 (是否拦截, 原因)。"""
        expression = str(config.get("匹配表达式", ""))
        is_whitelist = config.get("是否白名单", "否") == "是"
        scope = config.get("生效范围", "全部")
        table = str(standard.get("source_table", ""))
        operation = str(standard.get("operation", ""))

        if not expression:
            return False, ""
        if scope == "指定表":
            matched = expression in table
        elif scope == "指定操作":
            matched = expression in operation
        else:
            matched = expression in table or expression in operation

        if is_whitelist:
            # 白名单：不匹配则拦截
            return (not matched), (
                "" if matched else f"未命中白名单 {config.get('规则名称', '')}")
        # 黑名单：命中则拦截
        return matched, (f"命中黑名单 {config.get('规则名称', '')}" if matched else "")

    @staticmethod
    def _decision(standard: dict[str, Any], decision: str, overall: str,
                  changes: list[dict[str, Any]], extra: dict[str, Any]) -> dict[str, Any]:
        result = {
            "event_id": standard.get("event_id", ""),
            "tx_id": standard.get("tx_id", ""),
            "coordinate": standard.get("coordinate", ""),
            "decision": decision,
            "overall": overall,
            "rule_name": standard.get("rule_name", ""),
            "rule_version": standard.get("rule_version", 0),
            "changes": changes,
            "evaluated_at": datetime.now().isoformat(),
        }
        result.update(extra)
        return result

    # ------------------------------------------------------------------
    # 历史能力：原始事件黑白名单过滤（保持原有接口不变）
    # ------------------------------------------------------------------
    def apply_filter(self, event: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """对单个变更事件应用过滤规则。

        支持白名单(放行匹配)和黑名单(拦截匹配)模式，
        按优先级排序评估多条规则。
        """
        rule_name = config.get("规则名称", "")
        filter_type = config.get("过滤类型", "黑名单")
        expression = config.get("匹配表达式", "")
        is_whitelist = config.get("是否白名单", "否") == "是"
        priority = int(config.get("优先级", "0"))
        scope = config.get("生效范围", "全部")

        event_str = str(event)
        event_table = event.get("来源表", "")
        event_op = event.get("操作类型", "")

        matched = (
            expression in event_str
            or expression in event_table
            or expression in event_op
        )

        if matched and scope != "全部":
            if scope == "指定表" and expression not in event_table:
                matched = False
            elif scope == "指定操作" and expression not in event_op:
                matched = False

        passed = matched if is_whitelist else not matched

        pipeline_bus.filter_rules.append({
            "name": rule_name,
            "type": filter_type,
            "expression": expression,
            "is_whitelist": is_whitelist,
            "priority": priority,
            "enabled": config.get("status") == "已启用",
        })

        if not passed:
            pipeline_bus.pipeline_metrics["filtered"] += 1
            pipeline_bus.write_audit(
                "事件过滤", rule_name,
                f"事件被过滤规则 {rule_name} 拦截",
                "event_filter", "已过滤"
            )

        return {
            "event_id": event.get("事件ID", ""),
            "rule": rule_name,
            "filter_type": filter_type,
            "matched": matched,
            "passed": passed,
            "applied_at": datetime.now().isoformat(),
        }

    def batch_filter(self, events: list[dict[str, Any]], configs: list[dict[str, Any]]) -> dict[str, Any]:
        """批量过滤事件流。"""
        enabled_rules = sorted(
            [c for c in configs if c.get("status") == "已启用"],
            key=lambda c: int(c.get("优先级", "0")), reverse=True
        )
        passed: list[dict[str, Any]] = []
        blocked = 0
        for event in events:
            is_pass = True
            for rule in enabled_rules:
                filter_result = self.apply_filter(event, rule)
                if not filter_result["passed"]:
                    is_pass = False
                    break
            if is_pass:
                passed.append(event)
            else:
                blocked += 1
        return {
            "total": len(events),
            "passed": len(passed),
            "blocked": blocked,
            "pass_rate_pct": round(len(passed) / max(len(events), 1) * 100, 1),
        }

    def compute_filter_effectiveness(self, rule_name: str) -> dict[str, Any]:
        """计算过滤规则有效性。"""
        total_filtered = pipeline_bus.pipeline_metrics["filtered"]
        total_input = pipeline_bus.pipeline_metrics["total_input"]
        return {
            "rule": rule_name,
            "total_filtered": total_filtered,
            "total_input": total_input,
            "filter_rate_pct": round(total_filtered / max(total_input, 1) * 100, 1),
        }

    def validate_expression(self, expression: str, sample_event: dict[str, Any]) -> dict[str, Any]:
        """验证过滤表达式。"""
        event_str = str(sample_event)
        matched = expression in event_str
        return {
            "expression": expression,
            "syntax_valid": len(expression) > 0,
            "matches_sample": matched,
            "sample_used": sample_event.get("来源表", "none"),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用规则":
            pipeline_bus.filter_rules.append({
                "name": entry.get("规则名称"),
                "type": entry.get("过滤类型"),
                "expression": entry.get("匹配表达式"),
                "is_whitelist": entry.get("是否白名单", "否") == "是",
                "priority": int(entry.get("优先级", "0")),
                "enabled": True,
            })
            pipeline_bus.write_audit(
                "过滤启用", entry.get("规则名称", ""),
                f"过滤规则 {entry.get('规则名称')} 已启用",
                "event_filter", "已启用"
            )
        return entry, msg
