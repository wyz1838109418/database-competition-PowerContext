# PowerContext 2026 指定题目实现与复现

本实现基于官方 `2026-competition` 分支，保留冻结基线 `9db1753434162c0aa7c9f1189a1722e617880a52` 的提交历史。提交仓库为 `https://github.com/wyz1838109418/database-competition-PowerContext`。

## 实现范围

- 第一题通过官方公开 Fork 关系及原始提交历史准备评测仓库。
- 第二题在 `powercontext.sources.csv_rows` 实现严格离线 CSV 解析、稳定行 identity、CAPTURED Source、原字段顺序与文本证据投影。主键和值不裁剪、不自动转换类型。
- 第三题在 `powercontext.sources.pi_session` 实现 Pi v3 分支祖先链与完整可见事件解析。工具结果保留完整正文和 record，采用 ConnectorLifecycle 的持久化 checkpoint 去重与检测历史改写；不会执行会话中的命令。
- 第五题在 `powercontext.builtin.artifacts.memory.http_reranker` 实现调用方 AsyncClient 下的分批 HTTP 请求、严格响应检查、全局稳定排序及请求数报告。
- 第六题在 `powercontext.builtin.runtime.context_pack` 实现 UTF-8 有界分块、Base64 wire、SHA-256 与自包含字节计数、前缀增量和独立重建。`prepare_incremental` 调用正常 `prepare`，保留正文与引用语义。

## 公开验收

官方环境为 Linux x86_64、Python 3.12 和 competition/requirements.txt 中的固定依赖，不需要在线模型密钥。遵循 competition/README.md 安装后，依次运行：

```sh
make -C competition check-baseline
make -C competition test-csv
make -C competition test-pi
make -C competition test-rerank
make -C competition test-incremental
```

不要修改 competition/ 下的测试、工作负载、评分规则或锁定依赖。正式评测只加载 src/，自有测试和本文档不参与判题。

`tests/competition/test_selected_contract_edges.py` 补充了严格 CSV 错误、Pi 文本中的 Unicode 行分隔符、重复 JSON 字段、UTF-8 分块与 HTTP 布尔数值等边界测试。

## 提测材料

五题可使用同一仓库与同一完整 40 位提交 SHA。平台表单的代码仓库地址来自报名登记；填写分支 `competition/selected-tasks` 和实际提交 SHA。公开验收通过不代表正式隐藏验收或质量门已通过，最终状态以平台返回为准。

## 2026-10-26 自测记录

A1 Docker（Linux x86_64 / Python 3.12，官方固定依赖，运行时禁网、源码只读、1 CPU / 2 GiB）中，四组官方测试分别通过 17、15、10、14 项；自有边界测试通过 11 项。冻结基线祖先检查通过。依赖预下载后离线装入官方基础镜像，不改变评测依赖版本或 competition/ 文件。正式隐藏评测及第六题完整 40 轮成本门仍以平台结果为准。
