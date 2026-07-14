<template>
  <div class="card volatility-section">
    <div class="card-title">📉 波动率雷达图</div>
    <div ref="chartRef" class="chart-container"></div>
  </div>
</template>

<script setup>
import { ref, watch, onMounted, onUnmounted, nextTick } from 'vue'
import * as echarts from 'echarts'

const props = defineProps({
  data: { type: Object, default: null },
})

const chartRef = ref(null)
let chart = null
let resizeObserver = null

function buildOption() {
  if (!props.data || !props.data.data || props.data.data.length === 0) {
    return {}
  }

  const raw = props.data.data
  const names = raw.map(d => d.name)
  const avgVol = raw.map(d => d.avg_volatility * 10000)  // 放大便于显示
  const maxVol = raw.map(d => d.max_volatility * 10000)
  const maxDD = raw.map(d => d.max_drawdown_pct)

  return {
    tooltip: {
      trigger: 'item',
      backgroundColor: 'rgba(15,23,42,0.95)',
      borderColor: 'rgba(59,130,246,0.3)',
      textStyle: { color: '#e2e8f0', fontSize: 12 },
    },
    legend: {
      data: ['平均波动率', '最大波动率', '最大回撤%'],
      bottom: 0,
      textStyle: { color: '#94a3b8', fontSize: 10 },
    },
    grid: { left: '10%', right: '8%', top: '8%', bottom: '14%' },
    radar: {
      center: ['50%', '45%'],
      radius: '60%',
      indicator: names.map(n => ({ name: n, max: 10 })),
      axisName: { color: '#94a3b8', fontSize: 10 },
      splitArea: {
        areaStyle: {
          color: ['rgba(59,130,246,0.02)', 'rgba(59,130,246,0.02)'],
        },
      },
      splitLine: { lineStyle: { color: 'rgba(59,130,246,0.1)' } },
      axisLine: { lineStyle: { color: 'rgba(59,130,246,0.15)' } },
    },
    series: [
      {
        name: '波动率',
        type: 'radar',
        data: [
          {
            name: '平均波动率',
            value: avgVol,
            lineStyle: { color: '#60a5fa', width: 1 },
            areaStyle: { color: 'rgba(96,165,250,0.08)' },
            symbol: 'circle',
            symbolSize: 4,
          },
          {
            name: '最大波动率',
            value: maxVol,
            lineStyle: { color: '#f59e0b', width: 1 },
            areaStyle: { color: 'rgba(245,158,11,0.05)' },
            symbol: 'circle',
            symbolSize: 4,
          },
          {
            name: '最大回撤%',
            value: maxDD,
            lineStyle: { color: '#ef4444', width: 1, type: 'dashed' },
            symbol: 'diamond',
            symbolSize: 4,
          },
        ],
      },
    ],
  }
}

function initChart() {
  if (!chartRef.value) return
  chart = echarts.init(chartRef.value, null, { renderer: 'canvas' })
  chart.setOption(buildOption())
}

watch(() => props.data, () => {
  nextTick(() => {
    if (!chart) initChart()
    else chart.setOption(buildOption(), true)
  })
}, { deep: true })

onMounted(() => {
  nextTick(initChart)
  resizeObserver = new ResizeObserver(() => { if (chart) chart.resize() })
  if (chartRef.value) resizeObserver.observe(chartRef.value)
})

onUnmounted(() => {
  if (resizeObserver) resizeObserver.disconnect()
  if (chart) chart.dispose()
})
</script>
