# timebox — 对历史区间按时间切片重抓，与实时流合并，时间游标对齐、限速预算分配。

运行栈：Python 3.14 + 标准库 `sqlite3` + 可选 `httpx` + `pytest`（离线，无需联网）

## 运行方式
```text
make run       # 运行离线流水线，输出 {fed, rows}
make test      # 运行全部测试
make backfill  # 用 fixtures 演示历史回补（默认 2026-11-01 → 2026-11-04）
```

也可直接调用 CLI：

```text
python -m timebox.cli                      # fixture 流水线（行为与基线一致）
python -m timebox.cli backfill --start 2026-11-01 --end 2026-11-04 --unit day
python -m timebox.cli status  --start 2026-11-01 --end 2026-11-04
```

## 数据流与模块边界
`pipeline.py` 只做顺序编排，不含业务判断：

```text
loader → normalizer → timestep → merge → budget → cursor → keyer → store
```

历史回补走独立入口 `backfill.BackfillEngine`，同样只编排、不内嵌业务规则：

```text
gapfill.plan → timestep.slice_range → Source.fetch → merge.apply → budget.* → cursor.record
```

| 模块 | 单一职责 |
|---|---|
| `timestep` | 时间约定：半开区间、来源时区、夏令时、精度、按小时/按天切片 |
| `gapfill` | 覆盖判定：区间声明，四态分类，缺口规划，修订失效 |
| `backfill` | 回补编排：按缺口取片、预算预留/结算、合并、游标确认、重启恢复 |
| `merge` | 业务版本与来源证据：重传、修订、删除、冲突待审 |
| `budget` | 共享预算：预留/结算/退款逐笔可解释，lane 隔离，跨日、未知结果、重试 |
| `cursor` | 覆盖游标：逐片确认、续跑位置、一致性校验，绝不按最大游标推断覆盖 |

## 时间约定（全项目唯一约定）
- 区间**左闭右开** `[start, end)`；相邻切片首尾相接，不重不漏。
- 切片在**来源 IANA 时区**内进行：天边界是当地零点，小时边界是当地整点。
- 夏令时：重复的墙上时间解析为**两个** UTC 时刻；不存在的墙上时间跳过。因此按小时与按天拆分同一范围时覆盖完全相同（含 23/25 小时的日子）。
- `precision` 是调用方可接受的粒度，起止分别向下/向上取整后再切片，保证两种粒度结果一致。

## 覆盖状态（不能靠最大游标推断覆盖）
每个切片都写入区间声明，覆盖 = 有效声明区间的并集：

| 状态 | 含义 |
|---|---|
| `confirmed_data` | 抓取成功且有数据 |
| `confirmed_empty` | 抓取成功且来源明确回复无数据 |
| `pending` | 尚未抓取、可重试失败、结果未知，或来源修订已越过该声明 |
| `unrecoverable` | 来源明确表示该历史无法恢复 |

后续切片成功**不会**掩盖前面的缺口：`cursor.high_water()` 仅作参考，续跑位置取最早未确认点。
`RevisionMark` 抬高所需修订号，只有受影响区间回到 `pending`，其余保持确认。

## 回补与实时的取舍
- **版本优先**：更高业务版本总是胜出，来自历史或实时都一样；低版本重传记为 `stale` 并留档。
- 同版本同内容 = 幂等重投；同版本不同内容 = `conflict`，保持权威历史（默认 backfill）作为临时值**并进入待审队列**，两侧证据都保留。
- 删除也是版本：墓碑只有版本不低时才生效；更高版本的活记录会把墓碑复活。
- 因此：历史对“已发生的事实”权威，实时提供“及时性”，同版本矛盾既不盲信也不丢弃，交给待审。

## 预算
- 每条 `op_id` 可预留 `reserve` → `settle` / `refund`，每次状态变化都追加 ledger 明细，可逐笔解释。
- 重试同一 `op_id` 不会重复预留；同额结算幂等，异额结算报冲突。
- 结果未知（超时/连接中断）保留预留并标 `unknown`，重试复用同一预留；超过 `hold_ttl` 自动带原因退款，**不会长期阻塞实时流**。
- 实时 lane 可用全池；回补 lane 只能用 `capacity - realtime_reserve`，回补永远无法饿死实时。
- 预留记在创建时的账期内，跨日结算仍计入原账期。

## 一致性与恢复
- 游标是切片账本的投影（`cursor.rebuild`），`verify()` 校验：无覆盖漂移、确认区间不与待补重叠、记录的业务版本不低于合并状态。
- 重启只接续未确认或受修订影响的区间；已确认（含确认空、不可恢复）不重复抓取。
- `store.observations` 追加保留旧输入，覆盖当前视图不丢历史；`engine_state` 持久化切片账本、游标、预算与合并历史。
- 可查询：历史结果（`status.results`）、未补区间（`coverage.pending`）、恢复位置（`resume_position`）。

## 持久化与测试
SQLite 使用稳定键幂等写入；测试覆盖时间约定、缺口规划、预算账本、合并语义、游标不变量、重启恢复与 CLI。
运行方式以 `Makefile` 为准。
