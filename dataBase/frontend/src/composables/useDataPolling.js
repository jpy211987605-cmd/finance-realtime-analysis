/**
 * 数据轮询 Composable
 *
 * 定时从 FastAPI 后端拉取数据，管理加载状态和错误处理
 */
import { ref, reactive, onMounted, onUnmounted } from 'vue'
import axios from 'axios'

const API_BASE = '/api/realtime'

export function useDataPolling(intervalMs = 3000) {
  // ---- 状态 ----
  const latestData = ref(null)       // 最新行情快照
  const trendData = ref(null)        // 趋势数据（当前选中品种）
  const volumeData = ref(null)       // 成交量数据
  const riskData = ref(null)         // 风险指标
  const loading = ref(true)
  const error = ref(null)
  const lastUpdate = ref('')
  const selectedSymbol = ref('GOLD')

  let timer = null

  // ---- API 调用 ----
  async function fetchLatest() {
    try {
      const { data } = await axios.get(`${API_BASE}/latest`)
      latestData.value = data
      lastUpdate.value = data.update_time || new Date().toLocaleTimeString()
    } catch (e) {
      console.error('[Polling] latest 请求失败:', e.message)
    }
  }

  async function fetchTrend(symbol) {
    try {
      const { data } = await axios.get(`${API_BASE}/trend`, {
        params: { symbol, minutes: 60 },
      })
      trendData.value = data
    } catch (e) {
      console.error('[Polling] trend 请求失败:', e.message)
    }
  }

  async function fetchVolume() {
    try {
      const { data } = await axios.get(`${API_BASE}/volume`)
      volumeData.value = data
    } catch (e) {
      console.error('[Polling] volume 请求失败:', e.message)
    }
  }

  async function fetchRisk() {
    try {
      const { data } = await axios.get(`${API_BASE}/risk`)
      riskData.value = data
    } catch (e) {
      console.error('[Polling] risk 请求失败:', e.message)
    }
  }

  // ---- 全量刷新 ----
  async function refreshAll() {
    loading.value = true
    error.value = null
    try {
      await Promise.all([
        fetchLatest(),
        fetchTrend(selectedSymbol.value),
        fetchVolume(),
        fetchRisk(),
      ])
    } catch (e) {
      error.value = e.message
    } finally {
      loading.value = false
    }
  }

  // ---- 切换品种 ----
  function selectSymbol(symbol) {
    selectedSymbol.value = symbol
    fetchTrend(symbol)
  }

  // ---- 生命周期 ----
  onMounted(() => {
    refreshAll()
    timer = setInterval(refreshAll, intervalMs)
  })

  onUnmounted(() => {
    if (timer) clearInterval(timer)
  })

  return {
    latestData,
    trendData,
    volumeData,
    riskData,
    loading,
    error,
    lastUpdate,
    selectedSymbol,
    selectSymbol,
    refreshAll,
  }
}
