<template>
  <!-- 加载状态 -->
  <div v-if="loading" class="loading-overlay">
    <div class="loading-spinner"></div>
    <div class="loading-text">正在连接数据服务...</div>
  </div>

  <div class="dashboard">
    <!-- 顶部标题栏 -->
    <HeaderBar
      :last-update="lastUpdate"
      :connected="!error"
    />

    <!-- 实时行情卡片行 -->
    <div class="price-row">
      <PriceCard
        v-for="item in priceList"
        :key="item.symbol"
        :data="item"
        :is-active="item.symbol === selectedSymbol"
        @select="selectSymbol"
      />
    </div>

    <!-- 价格趋势图 (主图) -->
    <TrendChart
      :data="trendData"
      :name="currentSymbolName"
    />

    <!-- 成交量对比 -->
    <VolumeChart :data="volumeData" />

    <!-- 波动率雷达图 -->
    <VolatilityChart :data="riskData" />

    <!-- 风险监控面板 -->
    <RiskPanel :data="riskData" />
  </div>
</template>

<script setup>
import { computed } from 'vue'
import HeaderBar from './components/HeaderBar.vue'
import PriceCard from './components/PriceCard.vue'
import TrendChart from './components/TrendChart.vue'
import VolumeChart from './components/VolumeChart.vue'
import VolatilityChart from './components/VolatilityChart.vue'
import RiskPanel from './components/RiskPanel.vue'
import { useDataPolling } from './composables/useDataPolling.js'

const {
  latestData,
  trendData,
  volumeData,
  riskData,
  loading,
  error,
  lastUpdate,
  selectedSymbol,
  selectSymbol,
} = useDataPolling(3000)

const priceList = computed(() => {
  return latestData.value?.data || []
})

const currentSymbolName = computed(() => {
  const item = priceList.value.find(p => p.symbol === selectedSymbol.value)
  return item?.name || '--'
})
</script>
