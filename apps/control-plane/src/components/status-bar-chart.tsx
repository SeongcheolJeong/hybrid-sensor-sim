import ReactECharts from "echarts-for-react";

export function StatusBarChart({ title, rows }: { title: string; rows: Array<{ label: string; value: number }> }) {
  return (
    <ReactECharts
      style={{ height: 260 }}
      option={{
        backgroundColor: "transparent",
        title: {
          text: title,
          left: 0,
          textStyle: { color: "#eef2ff", fontSize: 14, fontWeight: 600 },
        },
        grid: { left: 72, right: 24, top: 44, bottom: 20 },
        xAxis: {
          type: "value",
          axisLabel: { color: "#94a3b8" },
          splitLine: { lineStyle: { color: "rgba(148,163,184,0.12)" } },
        },
        yAxis: {
          type: "category",
          data: rows.map((row) => row.label),
          axisLabel: { color: "#cbd5f5" },
        },
        series: [
          {
            type: "bar",
            data: rows.map((row) => row.value),
            itemStyle: { color: "#42d8ff", borderRadius: [0, 8, 8, 0] },
            label: { show: true, position: "right", color: "#eef2ff" },
          },
        ],
      }}
    />
  );
}
