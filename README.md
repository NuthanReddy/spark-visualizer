# Spark Visualizer · PySpark on YARN Interactive Explorer

An interactive architecture workbench that puts **all 54 PySpark-on-YARN components** on a spatial cluster canvas and traces RPCs, container allocations, Python worker sockets, shuffle data streams, and failure recovery hop-by-hop — inspired by [Protocol Explorer](https://protocol-explorer-alpha.vercel.app/).

Grounded in Apache Spark (`dada2a6`) and Apache Hadoop 3.4.1.

## Run locally

Requires Node.js 22+ (or Node 24) and npm.

```powershell
npm install
npm run dev
```

Open `http://127.0.0.1:5173`.

- `npm run build` — TypeScript check (`tsc -b`) + Vite production bundle (`dist/`)
- `npm test` — Validates all 54 components, pairwise non-overlap on the 1800×1040 master canvas, all 80 traces (6 lifecycle flows, 20 scenarios, 54 components), and playback geometry

## What you can explore

- **Three synchronized views** (`V` to cycle):
  1. **Topology Canvas** — Participating components grouped inside their real host/container boundaries with animated traveling packet orbs, glowing directional trails, ports, and protocol badges.
  2. **All 54 Cluster Map** — Every single PySpark-on-YARN component simultaneously on one zoomable, pannable architectural map. Watch cross-host traffic traverse Submit Host, YARN RM, AM/Driver JVM, Worker Node A (Map), Worker Node B (Reduce/Python/Write), and HDFS/History. Click any node to inspect its LLD or jump to its dedicated trace.
  3. **Sequence View** — Interactive timeline diagram synchronized 1:1 with the playback clock.
- **Step-by-step controls & Right-hand Step Details**:
  - Use **Prev Step** / **Next Step →** (or `←` / `→`, `Space` to play/pause, `S` to step one message) to walk through each trace step by step.
  - The right panel displays the active step's **command / RPC invocation**, **details passed (arguments, flags, headers, IDs)**, **wire payload preview**, and **sender → receiver state transitions**, alongside a **Component LLD** tab with lifecycle ownership, failure clues, and pinned Apache Spark source links.
- **80 interactive traces**:
  - **6 End-to-End Lifecycle Flows** (Cluster Mode, Client Mode & Discovery, Shuffle & Parquet Write, Python UDF Boundary, Dynamic Allocation, Shuffle `FetchFailed` Recovery)
  - **20 Scenarios** (matching `pyspark-on-yarn-scenarios.md` 01–20)
  - **54 Per-Component Traces** (one dedicated trace for every component)
- **URL deep-linking**: `#component=shuffle-reader`, `#scenario=11`, `#flow=flow-cluster-lifecycle`, plus `&view=cluster|topology|sequence`.

## Keyboard shortcuts

`Space` Play/Pause · `←` / `→` Prev/Next Step · `S` Step Message · `R` Reset · `[` / `]` Speed · `V` Switch View · `C` Categories · `G` Simulator · `L` Toggle Sidebar · `D` Diagram-Only · `T` Dark/Light Theme · `F` Fullscreen · `Ctrl+K` or `/` Search · `?` Shortcuts Help · `Esc` Close Modal.

## GitHub Pages

The public interactive site is hosted at [https://nuthanreddy.github.io/spark-visualizer/](https://nuthanreddy.github.io/spark-visualizer/). Pushes to `main` run the full unit/projection/simulator test suite, build the production bundle with `/spark-visualizer/` asset base, and deploy via GitHub Pages.
