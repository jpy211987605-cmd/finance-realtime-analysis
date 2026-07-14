<template>
  <div class="card trend-section">
    <div class="card-title">📈 实时价格趋势 — {{ name }}</div>
    <div ref="chartRef" class="chart-container"></div>
  </div>
</template>

<script setup>
import { ref, watch, onMounted, onUnmounted, nextTick } from 'vue'
import * as echarts from 'echarts'

const props = defineProps({
  data: { type: Object, default: null },
  name: { type: String, default: '' },
})

const chartRef = ref(null)
let chart = null
let resizeObserver = null

function buildOption() {
  if (!props.data || !props.data.data || props.data.data.length === 0) {
    return {}
  }

  const raw = props.data.data
  const times = raw.map(d => d.time)
  const prices = raw.map(d => d.price)
  const sma5 = raw.map(d => d.sma_5)
  const sma10 = raw.map(d => d.sma_10)
  const sma20 = raw.map(d => d.sma_20)

  return {
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(15,23,42,0.95)',
      borderColor: 'rgba(59,130,246,0.3)',
      textStyle: { color: '#e2e8f0', fontSize: 12 },
    },
    legend: {
      data: ['价格', 'SMA(5)', 'SMA(10)', 'SMA(20)'],
      bottom: 0,
      textStyle: { color: '#94a3b8', fontSize: 11 },
    },
    grid: { left: '3%', right: '5%', top: '8%', bottom: '14%' },
    xAxis: {
      type: 'category',
      data: times,
      axisLine: { lineStyle: { color: 'rgba(59,130,246,0.2)' } },
      axisTick: { show: false },
      axisLabel: {
        color: '#94a3b8',
        fontSize: 10,
        formatter: v => v.substring(11, 19) || v,
      },
    },
    yAxis: {
      type: 'value',
      scale: true,
      splitLine: { lineStyle: { color: 'rgba(59,130,246,0.1)' } },
      axisLabel: { color: '#94a3b8', fontSize: 10 },
    },
    dataZoom: [
      { type: 'inside', start: 60, end: 100 },
    ],
    series: [
      {
        name: '价格',
        type: 'line',
        data: prices,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 2, color: '#3b82f6' },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: 'rgba(59,130,246,0.25)' },
            { offset: 1, color: 'rgba(59,130,246,0.01)' },
          ]),
        },
      },
      {
        name: 'SMA(5)',
        type: 'line',
        data: sma5,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 1, color: '#f59e0b', type: 'dashed' },
      },
      {
        name: 'SMA(10)',
        type: 'line',
        data: sma10,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 1, color: '#8b5cf6', type: 'dashed' },
      },
      {
        name: 'SMA(20)',
        type: 'line',
        data: sma20,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 1, color: '#22c55e', type: 'dashed' },
      },
    ],
  }
}

function initChart() {
  if (!chartRef.value) return
  chart = echarts.init(chartRef.value, null, { renderer: 'canvas' })
  chart.setOption(buildOption())
}

function updateChart() {
  if (!chart) return
  chart.setOption(buildOption(), true)
}

watch(() => props.data, () => {
  nextTick(() => {
    if (!chart) initChart()
    else updateChart()
  })
}, { deep: true })

onMounted(() => {
  nextTick(initChart)
  resizeObserver = new ResizeObserver(() => {
    if (chart) chart.resize()
  })
  if (chartRef.value) resizeObserver.observe(chartRef.value)
})

onUnmounted(() => {
  if (resizeObserver) resizeObserver.disconnect()
  if (chart) chart.dispose()
})
</script>
