# Spark Visualizer -- Spark on YARN Interactive Explorer (PySpark, Scala, Java)

An interactive architecture workbench that puts **all 89 Spark-on-YARN components** (covering **PySpark [81/89], Scala [82/89], and Java [82/89]**) on a `2160 x 1260` spatial Master Canvas and traces RPCs, container allocations, Catalyst Analysis/Optimization, WholeStageCodegen, Janino CodeGenerator, Tungsten UnsafeRow, Typed Dataset Encoders & Aggregators, JVM/Python execution boundaries, shuffle data streams, push-based shuffle, node decommission migration, and failure recovery hop-by-hop -- inspired by [Protocol Explorer](https://protocol-explorer-alpha.vercel.app/).

Grounded in Apache Spark (`dada2a6`, `NuthanReddy/spark`) and Apache Hadoop 3.4.1.

## Run locally

Requires Node.js 22+ (or Node 24) and npm.

```powershell
npm install
npm run dev
```

Open `http://127.0.0.1:5173`.

- `npm run build` -- TypeScript check (`tsc -b`) + Vite production bundle (`dist/`)
- `npm test` -- Validates all 89 components (Scala 82 / Java 82 > PySpark 81), pairwise non-overlap on the `2160 x 1260` Master Canvas, all 128 curated traces (11 lifecycle flows, 28 scenarios, 89 component deep-dives), language routing (`#lang=`), host/process locality invariants, playback geometry, and the Multi-Language Simulator

## What you can explore

- **Global `PySpark | Scala | Java` Language Switcher & `#lang=` URL Routing**:
  - Switch globally between **PySpark (81/89)**, **Scala (82/89)**, and **Java (82/89)** (`#lang=pyspark|scala|java`).
  - Automatically adapts unified lifecycle flows, scenarios, and component deep-dives in place (e.g., Python Driver + Py4J vs JVM `--class` + `__app__.jar`, Python UDF worker IPC vs in-JVM Dataset Encoders, Typed Dataset Operators, Scala/Java UDFs, Typed Aggregators, ASM ClosureCleaner, KryoRegistrator, Scala REPL & Java Spark API), dims inactive components on the Master Canvas with corner language badges (`PY`, `JVM`, `SCALA`, `JAVA`), and adapts simulator jobs and faults.
- **Three synchronized views** (`V` to cycle):
  1. **Topology Canvas** -- Participating components grouped inside their real host/container boundaries with animated traveling packet orbs, glowing directional trails, ports, and protocol badges.
  2. **All 89 Master Canvas (`2160 x 1260`)** -- Every Spark-on-YARN component simultaneously on one zoomable, pannable architectural map across Submit Host, YARN Control, AM/Driver JVM, Worker Node A (Map/Memory/SerDe/Shuffle), Worker Node B (Codegen/Tungsten/UDF/Python/Write), and HDFS/History. Click any node to inspect its LLD or jump to its dedicated trace.
  3. **Sequence View** -- Interactive timeline diagram synchronized 1:1 with the playback clock.
- **Step-by-step controls & Right-hand Step Details**:
  - Use **Prev Step** / **Next Step** (or `Left` / `Right`, `Space` to play/pause, `S` to step one message) to walk through each trace step by step.
  - The right panel displays the active step's **command / RPC invocation**, **details passed (arguments, flags, headers, IDs)**, **wire payload preview**, and **sender -> receiver state transitions**, alongside a **Component LLD** tab with interactive **Peer Interactions** (`Receives From` / `Sends To`), lifecycle ownership, failure clues, and pinned Apache Spark source links.
- **128 curated traces** (zero duplicates):
  - **11 End-to-End Lifecycle Flows** (Full Cluster-Mode Batch Lifecycle, Client-Mode Startup & Discovery, End-to-End Shuffle & Parquet Write, Python Driver Py4J vs Executor UDF Worker / JVM Dataset Encoders & UDFs, Dynamic Allocation Scale-Up & Scale-Down, Shuffle `FetchFailed` Recovery, MetricsSystem vs Live SparkUI & History Server Lifecycles, Push-Based Shuffle with ESS ON, YARN Node Decommission & Block Migration with ESS OFF, Catalyst Analyzer -> Optimizer -> WholeStageCodegen & Tungsten, and Scala/Java Typed Dataset[T]: ClosureCleaner, MapElements, Aggregator & Kryo)
  - **28 Failure / Behavior Scenarios** (01--28, including Scala/Java ClassNotFound, Kryo buffer overflow, Java `Encoders.bean()` circular ref, Kerberos token renewal, Push-Based Shuffle, YARN Node Decommission, Non-Serializable Closure, and WholeStageCodegen Fallback + Memory Spill)
  - **89 Per-Component Deep-Dives** (one dedicated trace for every component)
- **Multi-Language Simulator** (`G` to open):
  - **8 job types** (Narrow Filter, Wide Shuffle Aggregation, Broadcast Hash Join, Skewed Sort-Merge Join with AQE, PySpark UDF Pipeline, Typed Dataset + JVM UDF, Cached RDD Multi-Action, and Driver `collect()`)
  - **5 data profiles** (Uniform, Hot-Key Skew, Small Dimension Table, Large Serialized Results, Malformed Row)
  - **1--6 dynamic executors**, configurable cores/partitions, cluster/client deploy mode, ESS, Dynamic Allocation, AQE, and Python UDF mode toggles
  - **10 fault categories** and **59 simulator modes** (happy path + **58 source-verified faults**, including all **10 SIGKILL-137** cases plus Heap OOM exit 52 and Node Crash contrast cases), with **16 one-click presets**
- **URL deep-linking**: `#component=shuffle-reader`, `#component=jvm-user-app&lang=scala`, `#scenario=11`, `#flow=flow-cluster-lifecycle`, `#sim=v1&job=wide-shuffle&fault=oom-pmem`, plus `&lang=pyspark|scala|java` and `&view=cluster|topology|sequence`.

## Keyboard shortcuts

`Space` Play/Pause | `Left` / `Right` Prev/Next Step | `S` Step Message | `R` Reset | `[` / `]` Speed | `V` Switch View | `C` Categories | `G` Simulator | `L` Toggle Sidebar | `D` Diagram-Only | `T` Dark/Light Theme | `F` Fullscreen | `Ctrl+K` or `/` Search | `?` Shortcuts Help | `Esc` Close Modal.

## GitHub Pages

The public interactive site is hosted at [https://nuthanreddy.github.io/spark-visualizer/](https://nuthanreddy.github.io/spark-visualizer/). Pushes to `main` run the full unit/projection/simulator test suite, build the production bundle with `/spark-visualizer/` asset base, and deploy via GitHub Pages.
