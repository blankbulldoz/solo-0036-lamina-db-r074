# eval/ — 备份选择实验工作区

本目录是在基线产品代码之外新增的**评估工作区**，不修改任何产品源码或备份格式。

## 目录约定（不可信生成数据隔离）

- `scripts/` — 自编排脚本，仅用 Python 标准库，可用 `python3 -I` 运行；不放在数据目录内。
- `bin/laminad` — 用当前源码在 Linux 上构建的静态二进制（`CGO_ENABLED=0`）。
  仓库根 `bin/laminad` 是 macOS arm64  Mach-O，无法在本机 Linux 运行，构建时未覆盖它。
- `runs/<id>/source|restored-*` — 全部由 `laminad` 自行创建的数据目录（不可信运行产物）。
- `runs/<id>/images/*.lmi` — `GET /backup` 返回的原始字节，落盘后不再改写，保持线上备份格式。
- `runs/<id>/report.json` — 机器可读证据：完整时序、逐序号探测、55 项独立交叉核对。

## 构建与复现

```sh
# 需要 Go 1.25（仓库 go.mod 声明 1.25.0）
CGO_ENABLED=0 go build -trimpath -o eval/bin/laminad ./cmd/laminad
python3 -I eval/scripts/backup_selection_experiment.py \
  --binary eval/bin/laminad \
  --run-dir eval/runs/<新运行目录> \
  --report eval/runs/<新运行目录>/report.json
```

脚本结束打印 `{"passed": true, ...}` 即全部交叉核对通过；结论与选择建议见
[`BACKUP_SELECTION.md`](./BACKUP_SELECTION.md)。
