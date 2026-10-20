# plaita-go

[![ci](https://github.com/jeffkit/plaita-go/actions/workflows/ci.yml/badge.svg)](https://github.com/jeffkit/plaita-go/actions/workflows/ci.yml)

plaita 的 Golang 引擎 + worker 运行时。内核为自底向上的 flow 引擎实现。

- 定位、决策、对齐矩阵、已知缺口：见 [AGENTS.md](AGENTS.md)
- worker 协议（队列/租约/键名/恢复语义）：见 [docs/WORKER_PROTOCOL.md](docs/WORKER_PROTOCOL.md)

```bash
make ci     # 与 ci.yml 同形：gofmt 检查 + vet + build + test + workflow 审计
make test   # 只跑测试；跳过清单与 CI 共用 scripts/ci-skip.txt
make lint   # gofmt 检查 + vet（golangci-lint 尚未接线）
```

状态（2026-10-21）：引擎内核、plaita 部署 IR 契约层、agentproc 节点、runtime worker（含 M6 挂起/恢复、
租约/续跑/死信/DLQ 回灌）、沙箱 driver 均已落地；未落地面与既有红点见 AGENTS.md「已知问题」。
