# CNKD Postgame Analytics 0.1.0

独立 NEXT 的赛后统计采集插件与私有报告服务。宿主需要 **CNKD NEXT 1.1.24 / Plugin API 1.3.0**；旧1.1.23保留为回退基线，不能直接加载本V2插件。

## 已实现

- 原生六类击杀/损失、收入速率/军队价值/建筑价值/总价值、模拟时间与个人结束状态；固定名单不以昵称为主键。
- 游戏线程只读复制，采集插件异步提交带SHA-256的原子日志块，持久化后ACK；独立服务事务导入、重复去重和冲突隔离。
- 结算前冻结终态，赛后才开放完整/部分/中断报告；原始分页、精确游标、个人范围、玩家/队伍共同时间网格和相对堆叠。
- 五页网页、深浅主题和手机布局；异步CSV/JSONL/GZIP/离线HTML导出、原始归档/恢复、固定和一致数据库备份。
- 默认只监听回环地址，管理凭据存于服务数据目录，浏览器以HttpOnly会话登录，不携带采集凭据。

**实现和验收分开**：局中“原生公式变化点”不是原生客户端历史下载。COMPLETE表示所定义服务端采集范围无已知缺口，不表示PC/安卓画面已逐点认证。真实两小时CPU/GC负载、实体客户端及完整玩法矩阵仍以交付验收记录为准。自然中断/磁盘满/进程强杀可能留下PARTIAL或INTERRUPTED，不会补造赢家和丢失尾段。

## 工程边界

| 模块 | 目标 |
| --- | --- |
| contracts | Java11，journal封装、校验及公共指标口径 |
| collector-plugin | Java11 V2插件；只依赖公开1.3.0 API，不打包宿主/引擎类 |
| report-service | JDK17独立进程，SQLite/API/导出/归档，内置独立静态页面 |

宿主源码独立维护；合作方源码包内为同级 `cnkd-next-host/`。RTSRank和portal仅作只读参考，本工程不依赖其房池、账号、积分、会员或生产配置。

## 构建

JDK17，在项目根执行：

```powershell
.\gradlew.bat test :collector-plugin:jar :report-service:jar --no-daemon
```

默认编译使用随源码附带的 `sdk/server-plugin-api-1.3.0.jar`，也可传 `-PcnkdApiJar=<1.3.0 API路径>`；不需要本机原始工作目录。JDK17与首次构建的Gradle/Maven依赖下载需要自行准备，已有缓存可加 `--offline`。运行已构建包不需要Gradle或重新下载Maven依赖。发布工具为 `tools/package.py --host-root <干净宿主发布目录> --host-source-root <宿主Git目录> --output <新输出目录>`；合作方无Git元数据的源码快照直接用于构建，重新制作带Git来源的包时需先建立自身版本库。

## 安装与启动

1. 停服备份旧JAR、data、插件和锁。替换为配套1.1.24宿主；保留现有配置和玩家数据。
2. 将 `cnkd-match-analytics-0.1.0.jar` 加入 `data/plugins/`，使用配套工具为**整个实际插件集合**生成批准锁。保留EIRS原件与其锁条目，不能将空模板锁覆盖现有插件锁。
3. 启动宿主。插件首次生成 `data/plugins/data/cnkd.match.analytics/config.json` 与 `journal/`。
4. 编辑分析服务 `config.json`，`journalDirectory` 指向上一步的绝对journal路径；`dataDirectory` 是分析服务自己的独立目录。
5. 运行 `java -jar cnkd-analytics-service-0.1.0.jar config.json`，打开 `http://127.0.0.1:28480/`。管理员凭据由服务生成在其 `dataDirectory/admin-token.txt`；不要公开它或把它放进前端。

服务配置示例：

```json
{
  "dataDirectory": "E:/CNKD-Analytics/data",
  "journalDirectory": "E:/CNKD-Server/data/plugins/data/cnkd.match.analytics/journal",
  "port": 28480,
  "maximumStorageMiB": 2048,
  "hotDays": 90,
  "archiveDays": 365
}
```

分析服务配置修改后重启服务生效，不重启游戏。远程网页接入需在回环服务外配置自己的受认证HTTPS入口；本服务不自动发布公网、不写反向代理配置。单节点只使用一个分析服务读取同一journal。

插件配置：`enabled=true`、`maximumJournalMiB=512`。可在控制台执行 `analytics status`、`analytics enable`、`analytics disable`、`analytics reload`。停用会让本局留下不完整标识，重新启用作用于下一局。最大journal容量修改需重启宿主；不要删除尚未入库的chunk文件。

默认按完整参赛名单复制原生值，最多100人。需要软预算时可在JVM参数设置 `-Dcnkd.statistics.copyBudgetMicros=2000`（范围0..50000，0为按名单全部复制）。轮转漏读会产生明确的逐玩家缺口，不把它称为完整。`analytics status`包含pending/ACK、复制平均/p99/最大耗时；真实长局仍须做A/B性能验收。

## 报告与数据口径

- 整局结束冻结后、记录提交SQLite成功才可查看。结算倒计时不是额外比赛时间；模拟时间与现实时间分开。
- 强制重置保留原始结果原因，不冒充自然胜利。进程退出后未收到终态的记录标为INTERRUPTED；活游戏仅因writer延迟不会被提前开放。
- 收入是原生速率，不是累计收入；总价值为军队与建筑价值之和。AI在名单中显式标记；观战者不作为参战者。
- 原始数值用于精确查询；显示图使用共同时间网格和联合极值精简。跨缺口显示未知；同值恢复后也保存恢复点，避免缺口污染后续正常时段。
- 个人终点为原生队伍结束观察，不等于本人UI触发帧。玩家/队伍合计均基于固定参赛者；协议身份不是认证真人账号，本版本没有跨局账号绑定。
- 归档为GZIP JSONL，校验hash、可解压和记录数量后才删除热曲线。恢复/导出流式处理，不沿用参考工程100万点的固定恢复上限；受配置存储预算约束。
- 导出异步生成，下载阶段不持有数据库锁。任务一小时过期；同时最多1个生成、4个等待、16个记录。超预算明确失败，原始记录保留。

## API

所有业务API需要管理员会话或独立服务Bearer凭据；不向普通游戏玩家或匿名用户开放全场数据。

| 路径 | 功能 |
| --- | --- |
| POST /api/session、/api/logout | 登录/退出，HttpOnly SameSite会话 |
| GET /api/status、/api/matches | 健康、采集错误与分页列表 |
| GET /api/matches/{id}、/{quality,comparison,cursor,raw,events} | 赛后详情、质量、共同网格、原始游标及分页 |
| POST /api/matches/{id}/{pin,archive,restore,backup} | 固定/归档/恢复/一致备份 |
| POST /api/matches/{id}/exports | 异步生成html/csv/jsonl/jsonl.gz |
| GET /api/exports/{jobId}、/{jobId}/download | 任务状态与文件下载 |

原始页最多1500点，支持afterGameMs；游标支持revision校验，拒绝迟到修订。导出CSV字段为participant/metric/simulationMs/value，JSONL首行为名单/计数/终态/质量清单；HTML为可离线查看的总览和四图预览，完整变化点随原始导出提供。

## 备份、回退和故障

网页“一致备份”创建 `data/backups/backup-<时间>/analytics.db` 和归档副本，并执行SQLite integrity_check。部署操作还应备份未确认journal、原始配置/管理凭据和批准锁。数据库恢复须停分析服务后在独立目录核对；不能覆盖后续玩家产生的数据。

回退：先停采集/宿主，恢复保存的1.1.23 JAR并只移除本插件对应批准项；EIRS和其锁条目保持。保留本版本journal、数据库及归档，不把新插件强行加载到旧API。网页服务可以独立停止，游戏照常运行；停太久会达到journal容量，必须查看状态，不保证无限缓存。

服务运行时输入 `exit` 可正常结束；宿主正常停服会先固定终态，再让插件提交剩余日志。进程强杀和断电恢复属于另一种保证，报告不会假装正常结束。

测试、构建、原生联调与未完成验收见发布包的 `VALIDATION-CN.md`。本地验证包不是生产部署收据。

## 可选玩家战报入口

默认关闭玩家查询，管理员可在私有页面创建24小时全场只读链接，并随时撤销本局所有链接。链接持有者可以阅读完整赛后报告，属于明确授予的全场只读权限；链接不是用户账号，不应公开转发。

需要游戏内 `.report` 或 `.战报` 时：在分析服务配置设置 `playerLinksEnabled=true` 并重启分析服务；将服务生成的 `collector-token.txt` 写入插件私有配置 `collectorToken`，设置 `playerReportsEnabled=true`、`reportServiceUrl=http://127.0.0.1:28480` 和自己的 `publicReportBaseUrl=https://报告域名`，再执行 `analytics reload`。外部HTTPS入口由服主配置，本地验证可用回环HTTP。请求只在插件自有的单线程4槽有界队列中执行，网络失败不阻塞游戏。

服务按固定开局协议身份摘要检查参赛资格，仅返回最近一场已结算报告的限时链接；未结算、未参赛、未配置或未开放时拒绝。链接凭据仅保存摘要，有效期24小时，可由管理页撤销；公开报告不包含内部协议ID或其摘要。采集应用凭据只能申请符合参赛身份的链接，不能管理配置、固定、归档或备份；不要向浏览器分发它。
