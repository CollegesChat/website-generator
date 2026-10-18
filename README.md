```mermaid
graph TD
    %% 阶段一：目录初始化与文档同步
    subgraph Phase1 [1. 初始化与下载阶段]
        A([脚本启动]) --> B[建立 content/docs 下universities、archived、choose-a-college 目录]
        B --> C[下载缺失的远程文档<br>出国受阻.md 等到 choose-a-college]
    end

    %% 阶段二：加载问卷定义与答卷数据
    subgraph Phase2 [2. 问卷与数据加载]
        C --> D[加载 v1 问卷定义 v1.yaml<br>失败则终止构建]
        D --> E[加载 v1 答卷 v1.csv<br>失败则终止构建]
        E --> F[追加人工导入批次 v1.additions.csv<br>拿不到就跳过]
        F --> G[加载省份高校映射<br>china-university-list output.csv]
        G --> H[加载 v2 问卷 v2.yaml 与答卷 v2.csv<br>拿不到则本次仅生成 v1]
        H --> I[按 v2.patches.yaml 处理脏数据]
    end

    %% 阶段三：按学校分组与归档判定
    subgraph Phase3 [3. 分组与时间判定]
        I --> J[按学校名分组<br>v1 取第 4 题、v2 取第 2 题]
        J --> K{提交时间在最近 3 年内?}
        K -- 是 --> L[归入活跃学校 active]
        K -- 否 --> M[归入已归档学校 archived]
    end

    %% 阶段四：渲染并写入 Hugo 页面
    subgraph Phase4 [4. 渲染与写入]
        L & M --> N[同校 v1 + v2 答卷合并<br>按答卷时间倒序]
        N --> O[清洗文件名 -> 生成拼音 Slug -> 查省份]
        O --> P[写入省份 _index.md<br>并逐校渲染正文 md]
        P --> Q([流程结束])
    end

    %% 样式调整
    style A fill:#4CAF50,stroke:#333,stroke-width:2px,color:#fff
    style Q fill:#F44336,stroke:#333,stroke-width:2px,color:#fff
    style K fill:#FF9800,stroke:#333,stroke-width:2px,color:#fff
    style H fill:#FF9800,stroke:#333,stroke-width:2px,color:#fff
```

> [!NOTE]
> `debug` 子命令走同一条渲染管线，只是数据源换成本地 mock，不做下载与归档切分。

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
