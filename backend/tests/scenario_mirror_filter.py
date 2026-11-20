"""端到端场景验证：sales.orders 同一事务两条 UPDATE 的镜像适配与字段级过滤。

覆盖断言（对应任务要求）：
1. 标准镜像区分显式 null 与源记录缺字段，值转换只作用于实际出现的字段；
2. 事务首条订单变更到达时绑定规则版本，整笔事务固定同一版（版本交接不影响在途事务）；
3. 规则 v1：第一条 UPDATE 缺 discount → 按默认值 0 产生“默认值更新”；
   规则 v2：缺 discount 不填，不产生任何 discount 变更；
4. 第二条 UPDATE 显式 discount: null → 产生清除（clear），且缺字段不会被当成清除；
5. 同源坐标重复提交不再放行；已下发事务可按原规则回查，整笔重放结果稳定；
6. 无变动数据保留、黑白名单拦截正常。

以 python -I 运行（隔离模式），路径由脚本自行注入，不依赖当前工作目录。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from app.pipeline import pipeline_bus  # noqa: E402
from app.services.format_adapter import FormatAdapterService  # noqa: E402
from app.services.event_filter import EventFilterService  # noqa: E402

RULE = "orders-mirror-rule"
TX = "tx-1001"
COLUMNS = ["id", "status", "discount", "amount"]

# sales.orders 第一条 UPDATE：只改 status，源记录根本没传 discount
EVENT_1 = {
    "事件ID": "evt-1",
    "来源表": "sales.orders",
    "操作类型": "UPDATE",
    "变更前镜像": json.dumps({"id": 9, "status": "paid", "discount": 12.5, "amount": 100}),
    "变更后镜像": json.dumps({"id": 9, "status": "shipped", "amount": 100}),
    "事务ID": TX,
    "事件时间": "2026-11-20T10:00:00",
    "binlog_file": "mysql-bin.000007",
    "binlog_pos": 4100,
    "columns": COLUMNS,
}

# sales.orders 第二条 UPDATE：明确把 discount 清空（显式 null）
EVENT_2 = {
    "事件ID": "evt-2",
    "来源表": "sales.orders",
    "操作类型": "UPDATE",
    "变更前镜像": json.dumps({"id": 9, "status": "shipped", "discount": 0, "amount": 100}),
    "变更后镜像": json.dumps({"id": 9, "status": "shipped", "discount": None, "amount": 100}),
    "事务ID": TX,
    "事件时间": "2026-11-20T10:00:01",
    "binlog_file": "mysql-bin.000007",
    "binlog_pos": 4180,
    "columns": COLUMNS,
}

BLACKLIST = {
    "规则名称": "block-tmp",
    "过滤类型": "黑名单",
    "匹配表达式": "sales.tmp_orders",
    "是否白名单": "否",
    "生效范围": "指定表",
    "优先级": "10",
}

failures: list[str] = []


def check(label: str, condition: bool, detail: object = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {label}" + (f" :: {detail}" if detail and not condition else ""))
    if not condition:
        failures.append(label)


def main() -> int:
    adapter = FormatAdapterService()
    flt = EventFilterService()
    pipeline_bus.reset_runtime()

    # --------------------------------------------------------------
    # 规则 v1：discount 缺字段默认 0；amount 保留两位小数
    # --------------------------------------------------------------
    v1 = adapter.register_rule_version({
        "规则名称": RULE,
        "输入格式": "debezium",
        "输出格式": "canal",
        "字段重命名": "",
        "值转换": "amount:round:2",
        "缺省填充": "discount:0",
        "时间戳格式": "%Y-%m-%d %H:%M:%S",
        "是否压缩": "否",
    })
    check("v1 登记为第 1 版", v1["version"] == 1, v1)

    # 第一条订单变更到达，事务在此刻绑定 v1
    std1_v1 = adapter.build_standard_mirror(EVENT_1, v1)
    binding = pipeline_bus.bind_transaction_rule(TX, RULE)
    check("事务首条事件绑定 v1", binding["version"] == 1 and binding["first_bind"] is True)

    check("标准镜像保留显式缺字段标记",
          "discount" in std1_v1["missing_after"] and std1_v1["after"].get("discount") == 0)
    check("缺省填充被标注为 default_filled",
          std1_v1["default_filled_after"].get("discount") == 0)
    check("值转换规则作用于实际字段(amount round:2)",
          std1_v1["after"]["amount"] == 100.0 and std1_v1["before"]["discount"] == 12.5)

    # 版本交接：登记 v2（缺 discount 不填），并切换启用版本
    v2 = adapter.register_rule_version({
        "规则名称": RULE,
        "输入格式": "debezium",
        "输出格式": "canal",
        "字段重命名": "",
        "值转换": "amount:round:2",
        "缺省填充": "",  # 缺字段默认值从 0 改成不填
        "时间戳格式": "%Y-%m-%d %H:%M:%S",
        "是否压缩": "否",
    })
    check("v2 登记为第 2 版且已切换启用",
          v2["version"] == 2 and adapter.get_active_rule(RULE)["version"] == 2)

    # 同一事务第二条事件随后到达：必须仍用 v1，而不是启用中的 v2
    std2 = adapter.adapt_transaction([EVENT_2], RULE)[0]
    check("版本交接不影响在途事务：第二条仍按 v1 转换",
          std2["rule_version"] == 1, std2["rule_version"])
    check("显式 null 原样保留，与缺字段区分",
          std2["after"].get("discount") is None
          and "discount" in std2["null_after"]
          and "discount" not in std2["missing_after"])

    # 用绑定版本重算第一条，保证同一事务所有行同一版规则
    std1 = adapter.adapt_transaction([EVENT_1], RULE)[0]
    check("同一事务所有行固定同一版规则(v1)",
          std1["rule_version"] == 1 and std2["rule_version"] == 1)

    # --------------------------------------------------------------
    # 事件过滤：消费标准镜像决定字段变更放行
    # --------------------------------------------------------------
    d1 = flt.filter_standard_event(std1)
    check("第一条 UPDATE 放行", d1["decision"] == "emit" and d1["overall"] == "change")
    check("第一条发出默认值更新 discount=0",
          d1["payload"]["default_updates"] == {"discount": 0}
          and d1["payload"]["update_fields"].get("discount") == 0
          and d1["payload"]["update_fields"].get("status") == "shipped",
          d1["payload"])
    check("无变动字段保留在 unchanged 且不带值",
          "amount" in d1["payload"]["unchanged_fields"]
          and "id" in d1["payload"]["unchanged_fields"])

    d2 = flt.filter_standard_event(std2)
    check("第二条 UPDATE 放行并识别为清除",
          d2["decision"] == "emit"
          and any(c["change_kind"] == "clear" and c["field"] == "discount"
                  for c in d2["changes"]))
    check("第二条实际发出 discount 清除(null)",
          d2["payload"]["clear_fields"] == {"discount": None}, d2["payload"])
    check("清除行没有默认值更新/普通更新",
          d2["payload"]["update_fields"] == {} and d2["payload"]["default_updates"] == {})

    # 提交事务档案
    bundle = flt.commit_transaction([std1, std2], [d1, d2])
    check("事务档案按 v1 归档",
          bundle["rule_version"] == 1 and len(bundle["records"]) == 2)
    check("已下发事务可按原规则回查",
          pipeline_bus.get_dispatched_transaction(TX)["tx_id"] == TX)

    # --------------------------------------------------------------
    # 同源坐标重复提交不再放行
    # --------------------------------------------------------------
    dup = flt.filter_standard_event(std1)
    check("同源坐标重复提交被拦截",
          dup["decision"] == "block" and dup["overall"] == "duplicate_coordinate", dup)

    # --------------------------------------------------------------
    # 整笔重放：按原规则 v1，结果与首次一致
    # --------------------------------------------------------------
    replay = flt.replay_transaction(TX)
    check("重放使用事务绑定的原版本 v1", replay["rule_version"] == 1)
    replay_payloads = replay["emitted_payloads"]
    check("重放默认值更新与首次一致",
          replay_payloads[0]["default_updates"] == {"discount": 0}
          and replay_payloads[0]["update_fields"].get("discount") == 0
          and replay_payloads[0]["update_fields"].get("status") == "shipped")
    check("重放清除与首次一致",
          replay_payloads[1]["clear_fields"] == {"discount": None})
    check("重放两行均放行且版本一致",
          all(d["rule_version"] == 1 for d in replay["decisions"])
          and all(d["decision"] == "emit" for d in replay["decisions"]))

    # --------------------------------------------------------------
    # 新事务在 v2 下：缺 discount 不填，不产生 discount 变更
    # --------------------------------------------------------------
    pipeline_bus.dispatched_coordinates.clear()  # 新事务使用新坐标，仅清空坐标便于独立验证
    event1_v2 = dict(EVENT_1, 事件ID="evt-3", 事务ID="tx-2002", binlog_pos=5200)
    std_v2 = adapter.adapt_transaction([event1_v2], RULE)[0]
    check("新事务绑定当前启用 v2", std_v2["rule_version"] == 2)
    check("v2 下缺 discount 不填且保持缺字段标记",
          "discount" not in std_v2["after"]
          and "discount" in std_v2["missing_after"]
          and std_v2["default_filled_after"] == {})
    d_v2 = flt.filter_standard_event(std_v2)
    check("v2 下缺字段不产生任何 discount 变更",
          all(c["field"] != "discount" for c in d_v2["changes"])
          and d_v2["payload"]["update_fields"] == {"status": "shipped"},
          d_v2["payload"])

    # --------------------------------------------------------------
    # 显式 null 与缺字段语义边界 + 无变动保留 + 黑名单拦截 + INSERT
    # --------------------------------------------------------------
    # 缺字段绝不能被当成清除：构造缺 discount 的全新 UPDATE
    missing_only = {
        "事件ID": "evt-4",
        "来源表": "sales.orders",
        "操作类型": "UPDATE",
        "变更前镜像": json.dumps({"id": 10, "status": "paid", "discount": 5, "amount": 80}),
        "变更后镜像": json.dumps({"id": 10, "status": "paid", "amount": 80}),
        "事务ID": "tx-3003",
        "事件时间": "2026-11-20T10:01:00",
        "binlog_file": "mysql-bin.000007",
        "binlog_pos": 6300,
        "columns": COLUMNS,
    }
    std_missing = adapter.adapt_transaction([missing_only], RULE)[0]
    d_missing = flt.filter_standard_event(std_missing)
    check("缺字段不产生清除也不产生更新",
          d_missing["payload"]["clear_fields"] == {}
          and "discount" not in d_missing["payload"]["update_fields"])
    check("全部无变动时保留 none 记录",
          d_missing["decision"] == "emit" and d_missing["overall"] == "none"
          and set(d_missing["payload"]["unchanged_fields"]) == {"id", "status", "amount"})

    # 黑名单表拦截
    blocked_event = dict(EVENT_1, 事件ID="evt-5", 来源表="sales.tmp_orders",
                         事务ID="tx-4004", binlog_pos=7400)
    std_block = adapter.adapt_transaction([blocked_event], RULE)[0]
    d_block = flt.filter_standard_event(std_block, BLACKLIST)
    check("黑名单表事件被拦截",
          d_block["decision"] == "block" and d_block["overall"] == "rule_blocked", d_block)

    # INSERT 事件分类
    insert_event = {
        "事件ID": "evt-6",
        "来源表": "sales.orders",
        "操作类型": "INSERT",
        "变更前镜像": "{}",
        "变更后镜像": json.dumps({"id": 11, "status": "new", "discount": None, "amount": 50}),
        "事务ID": "tx-5005",
        "事件时间": "2026-11-20T10:02:00",
        "binlog_file": "mysql-bin.000007",
        "binlog_pos": 8500,
        "columns": COLUMNS,
    }
    std_ins = adapter.adapt_transaction([insert_event], RULE)[0]
    d_ins = flt.filter_standard_event(std_ins)
    check("INSERT 放行且显式 null 不被转换/填充",
          d_ins["decision"] == "emit" and d_ins["overall"] == "insert"
          and std_ins["after"]["discount"] is None
          and "discount" not in std_ins["default_filled_after"])

    print("\n标准镜像示例（第一条 UPDATE，v1）：")
    print(json.dumps(std1, ensure_ascii=False, indent=2, default=str))
    print("\n过滤载荷示例（默认值更新 / 清除）：")
    print(json.dumps([d1["payload"], d2["payload"]], ensure_ascii=False, indent=2))

    if failures:
        print(f"\n{len(failures)} 项断言失败：{failures}")
        return 1
    print("\n全部断言通过。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
