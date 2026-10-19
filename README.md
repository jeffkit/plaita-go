# plaita-go

[![ci](https://github.com/jeffkit/plaita-go/actions/workflows/ci.yml/badge.svg)](https://github.com/jeffkit/plaita-go/actions/workflows/ci.yml)

plaita 的 Golang 引擎 + worker 运行时。内核为自底向上的 flow 引擎实现。

- 定位、决策、对齐矩阵：见 [AGENTS.md](AGENTS.md)
- 出处与清理记录：见 []()

```bash
make test   # 或 GOPROXY=https://goproxy.cn,direct go test ./...
```

状态：骨架期。引擎内核可用（flow JSON → 节点执行），plaita 部署 IR 契约层、agentproc 节点、runtime worker 尚未开始。
