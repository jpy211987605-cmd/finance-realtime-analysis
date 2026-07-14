<template>
  <div class="header card">
    <div class="header-left">
      <span class="header-icon">🏦</span>
      <h1>金融实时数据分析大屏</h1>
    </div>
    <div class="header-right">
      <div class="header-badge">
        📡 状态: <span>{{ connected ? '在线' : '离线' }}</span>
      </div>
      <div class="header-badge">
        🕐 更新: <span>{{ lastUpdate }}</span>
      </div>
      <div class="header-badge">
        📊 品种: <span>5</span>
      </div>
    </div>
  </div>
</template>

<script setup>
defineProps({
  lastUpdate: { type: String, default: '--' },
  connected: { type: Boolean, default: false },
})
</script>
