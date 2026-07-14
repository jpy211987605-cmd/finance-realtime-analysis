<template>
  <div
    class="card price-card"
    :class="{ active: isActive }"
    :style="{ borderLeftColor: data.color }"
    @click="$emit('select', data.symbol)"
  >
    <div class="symbol-name">{{ data.name }} ({{ data.symbol }})</div>
    <div class="price-value" :style="{ color: data.color }">
      {{ formatPrice(data.price) }}
    </div>
    <div>
      <span
        class="price-change"
        :class="{
          up: data.price_change_pct > 0,
          down: data.price_change_pct < 0,
          flat: data.price_change_pct === 0,
        }"
      >
        {{ data.price_change_pct > 0 ? '↑' : data.price_change_pct < 0 ? '↓' : '→' }}
        {{ formatPct(data.price_change_pct) }}
      </span>
    </div>
    <div class="volume-info">
      VOL: {{ formatVolume(data.volume) }} | 波动: {{ formatVol(data.volatility) }}
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  data: { type: Object, required: true },
  isActive: { type: Boolean, default: false },
})

defineEmits(['select'])

function formatPrice(v) {
  if (v == null) return '--'
  if (Math.abs(v) < 10) return v.toFixed(4)
  if (Math.abs(v) < 100) return v.toFixed(2)
  return v.toFixed(1)
}

function formatPct(v) {
  if (v == null) return '0.00%'
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(2)}%`
}

function formatVolume(v) {
  if (v == null) return '--'
  if (v >= 10000) return (v / 10000).toFixed(1) + '万'
  if (v >= 1000) return (v / 1000).toFixed(1) + 'K'
  return v.toFixed(0)
}

function formatVol(v) {
  if (v == null) return '--'
  return (v * 100).toFixed(4) + '%'
}
</script>
