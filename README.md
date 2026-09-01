```mermaid
graph TD
    %% 阶段一：环境初始化与文件下载
    subgraph Phase1 [1. 初始化与下载阶段]
        A([脚本启动]) --> B[建立必要的本地目录结构]
        B --> C[下载远程资源文件<br>colleges.csv, results_desensitized.csv等]
        C --> D[下载文档及首页 index.md<br>并重命名为 Hugo 格式的 _index.md]
    end

    %% 阶段二：数据读取与初次归类
    subgraph Phase2 [2. 数据读取与时间判定]
        D --> E[解析省份与高校映射表 colleges.csv]
        E --> F[逐行读取问卷结果 results_desensitized.csv]
        F --> G{提交时间在最近3年内?}
        G -- 否 (3年前及更早) --> H[加载至已归档字典<br>universities_archived]
        G -- 是 (最近3年内) --> I[加载至活跃字典<br>universities]
    end

    %% 阶段三：别名整合与黑白名单过滤
    subgraph Phase3 [3. 数据清洗与关联]
        H & I --> J{是否开启 Debug 模式?}
        J -- 是 --> K[两张表各随机抽样 100 条数据]
        J -- 否 --> L[保留完整数据]
        K & L --> M[整合别名 alias.txt<br>将别名高校数据合并到主校名]
        M --> N[过滤黑名单 blacklist.txt<br>并对非正规校名发出警告]
    end

    %% 阶段四：多线程渲染 Markdown
    subgraph Phase4 [4. 并发渲染与写入]
        N --> O[初始化 FileNameMap<br>生成唯一的拼音 Slug]
        O --> P[通过 ThreadPoolExecutor<br>并发执行 Markdown 写入]
        P --> Q[清理文件名 -> 映射省份 -> 拼装 Hugo Front Matter]
        Q --> R[并行写入 .md 文件到对应省份目录]
        R --> S([流程结束])
    end

    %% 样式调整
    style A fill:#4CAF50,stroke:#333,stroke-width:2px,color:#fff
    style S fill:#F44336,stroke:#333,stroke-width:2px,color:#fff
    style G fill:#FF9800,stroke:#333,stroke-width:2px,color:#fff
    style J fill:#FF9800,stroke:#333,stroke-width:2px,color:#fff
```

## Debug

使用 mock 数据生成 v2 报告：

```bash
python -m generator debug
```

也可以在 `debug` 后传入一份问卷星导出的 CSV 或 Excel 文件，文件中的答卷会按学校合并到 mock 数据中：

```bash
python -m generator debug ./responses.csv
python -m generator debug ./responses.xlsx
```
