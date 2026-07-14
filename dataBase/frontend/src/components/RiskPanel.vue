<template>
  <div class="card risk-section">
    <div class="card-title">⚠️ 风险指标监控</div>
    <div class="risk-grid">
      <div
        v-for="item in riskList"
        :key="item.symbol"
        class="risk-item"
      >
        <div class="risk-label" :style="{ color: item.color }">
          {{ item.name }} ({{ item.symbol }})
        </div>
        <div class="risk-value" :style="{ color: item.color }">
          {{ item.maxDrawdown }}%
        </div>
        <div class="risk-sub">最大回撤</div>
        <div style="margin-top: 8px; font-size: 11px; color: #64748b;">
          波动率 {{ item.avgVolatility }} | 价差 {{ item.avgSpread }}
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  data: { type: Object, default: null },
})

const riskList = computed(() => {
  if (!props.data || !props.data.data) return []
  return props.data.data.map(d => ({
    symbol: d.symbol,
    name: d.name,
    color: d.color,
    maxDrawdown: d.max_drawdown_pct?.toFixed(3) || '--',
    avgVolatility: d.avg_volatility ? (d.avg_volatility * 100).toFixed(4) + '%' : '--',
    avgSpread: d.avg_spread?.toFixed(4) || '--',
  }))
})
</script>
