<template>
  <div class="card volume-section">
    <div class="card-title">📊 成交量对比</div>
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
  const volumes = raw.map(d => d.total_volume)
  const colors = raw.map(d => d.color)
  const avgVolumes = raw.map(d => d.avg_volume)

  return {
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(15,23,42,0.95)',
      borderColor: 'rgba(59,130,246,0.3)',
      textStyle: { color: '#e2e8f0', fontSize: 12 },
      formatter: params => {
        const p = params[0]
        return `${p.name}<br/>总成交量: ${(p.value/10000).toFixed(1)}万`
      },
    },
    grid: { left: '10%', right: '8%', top: '5%', bottom: '10%' },
    xAxis: {
      type: 'category',
      data: names,
      axisLabel: { color: '#94a3b8', fontSize: 11 },
      axisTick: { show: false },
      axisLine: { lineStyle: { color: 'rgba(59,130,246,0.2)' } },
    },
    yAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: 'rgba(59,130,246,0.1)' } },
      axisLabel: {
        color: '#94a3b8',
        fontSize: 10,
        formatter: v => (v / 10000).toFixed(0) + '万',
      },
    },
    series: [
      {
        type: 'bar',
        data: volumes.map((v, i) => ({
          value: v,
          itemStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: colors[i] || '#3b82f6' },
              { offset: 1, color: 'rgba(15,23,42,0.5)' },
            ]),
            borderRadius: [4, 4, 0, 0],
          },
        })),
        barWidth: '45%',
        emphasis: {
          itemStyle: { shadowBlur: 10, shadowColor: 'rgba(59,130,246,0.5)' },
        },
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
