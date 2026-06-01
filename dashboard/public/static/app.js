/* ============================================================
   TheLook eCommerce — KPI Dashboard  |  app.js
   ============================================================ */

const DATA_URL = '/data/dashboard_data.json'
let D = null          // global data store
let charts = {}       // chart instances keyed by canvas id

const fmt = {
  currency: v => '$' + Number(v).toLocaleString('en-US', { maximumFractionDigits: 0 }),
  currencyK: v => {
    const n = Number(v)
    if (n >= 1e6) return '$' + (n / 1e6).toFixed(1) + 'M'
    if (n >= 1e3) return '$' + (n / 1e3).toFixed(0) + 'K'
    return '$' + n.toFixed(0)
  },
  number: v => Number(v).toLocaleString('en-US'),
  pct:    v => Number(v).toFixed(1) + '%',
  days:   v => Math.round(Number(v)) + 'd',
}

// ── Colour palettes ──────────────────────────────────────────
const PALETTE = {
  indigo:  '#6366f1', emerald: '#10b981', yellow: '#f59e0b',
  red:     '#ef4444', sky:     '#0ea5e9', pink:   '#ec4899',
  violet:  '#8b5cf6', teal:    '#14b8a6', orange: '#f97316',
  lime:    '#84cc16',
}
const SEG_COLORS = {
  'Champions':         '#6366f1',
  'Loyal Customers':   '#10b981',
  'Regular':           '#0ea5e9',
  'At Risk':           '#f59e0b',
  'Potential Loyalists':'#8b5cf6',
  'Lost':              '#ef4444',
}
const CHART_DEFAULTS = {
  color:        '#9ca3af',
  gridColor:    'rgba(255,255,255,0.05)',
  borderColor:  '#1f2937',
}

Chart.defaults.color       = CHART_DEFAULTS.color
Chart.defaults.borderColor = CHART_DEFAULTS.borderColor

function makeChart(id, config) {
  if (charts[id]) { charts[id].destroy() }
  const canvas = document.getElementById(id)
  if (!canvas) return null
  charts[id] = new Chart(canvas, config)
  return charts[id]
}

// ── Boot ─────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', async () => {
  try {
    const res = await fetch(DATA_URL)
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    D = await res.json()
    init()
  } catch (e) {
    document.getElementById('loading').classList.add('hidden')
    const err = document.getElementById('error-state')
    err.classList.remove('hidden')
    document.getElementById('error-msg').textContent = e.message
  }
})

function init() {
  document.getElementById('loading').classList.add('hidden')
  document.getElementById('dashboard').classList.remove('hidden')

  // Last updated
  const exp = new Date(D.exported_at)
  document.querySelector('#last-updated span').textContent =
    exp.toLocaleDateString('en-US', { month:'short', day:'numeric', year:'numeric' })

  // Data range from annual_revenue
  if (D.annual_revenue?.length) {
    const years = D.annual_revenue.map(r => r.year)
    document.getElementById('data-range').textContent =
      `${Math.min(...years)} – ${Math.max(...years)}`
  }

  // Pipeline quality badge
  if (D.quality_summary) {
    const q = D.quality_summary
    const badge = document.getElementById('pipeline-badge')
    badge.classList.remove('hidden')
    badge.classList.add('flex')
    if (q.failed === 0) {
      badge.className = badge.className + ' bg-emerald-900/50 text-emerald-400 border border-emerald-800'
      badge.innerHTML = '<i class="fas fa-check-circle"></i> Pipeline Healthy'
    } else {
      badge.className = badge.className + ' bg-red-900/50 text-red-400 border border-red-800'
      badge.innerHTML = `<i class="fas fa-exclamation-circle"></i> ${q.failed} Tests Failed`
    }
  }

  // Nav
  document.querySelectorAll('.nav-link').forEach(link => {
    link.addEventListener('click', e => {
      e.preventDefault()
      const sec = link.dataset.section
      switchSection(sec, link)
    })
  })

  buildOverview()
}

// ── Section switching ─────────────────────────────────────────
const TITLES = {
  overview:  ['Overview',     'All-time KPIs & trends'],
  revenue:   ['Revenue',      'Monthly & category breakdown'],
  customers: ['Customers',    'RFM segmentation'],
  cohorts:   ['Cohorts',      'Retention heatmap'],
  products:  ['Products',     'Top 20 by revenue'],
  geo:       ['Geography',    'Revenue by country'],
  quality:   ['Data Quality', '31 automated tests'],
}
const BUILDERS = {
  overview:  buildOverview,
  revenue:   buildRevenue,
  customers: buildCustomers,
  cohorts:   buildCohorts,
  products:  buildProducts,
  geo:       buildGeo,
  quality:   buildQuality,
}
let currentSection = 'overview'
let builtSections = new Set()

function switchSection(sec, linkEl) {
  document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'))
  if (linkEl) linkEl.classList.add('active')

  document.querySelectorAll('section[id^="section-"]').forEach(s => s.classList.add('hidden'))
  document.getElementById(`section-${sec}`)?.classList.remove('hidden')

  const [title, sub] = TITLES[sec] || [sec, '']
  document.getElementById('page-title').textContent = title
  document.getElementById('page-sub').textContent   = 'TheLook eCommerce · ' + sub

  if (!builtSections.has(sec) && BUILDERS[sec]) {
    BUILDERS[sec]()
    builtSections.add(sec)
  }
  currentSection = sec
}

// ═══════════════════════════════════════════════════════════════
// OVERVIEW
// ═══════════════════════════════════════════════════════════════
function buildOverview() {
  const k = D.kpis
  const kpiDefs = [
    { label:'Gross Revenue',   value: fmt.currencyK(k.gross_revenue),   icon:'fa-dollar-sign', color:'indigo', sub:`Net: ${fmt.currencyK(k.net_revenue)}`,     badge:`+${fmt.pct(k.yoy_growth_pct)} YoY`, badgeType:'up' },
    { label:'Gross Profit',    value: fmt.currencyK(k.gross_profit),    icon:'fa-chart-pie',   color:'emerald',sub:`Margin: ${fmt.pct(k.overall_margin_pct)}`,  badge:'Margin', badgeType:'neutral' },
    { label:'Total Orders',    value: fmt.number(k.total_orders),       icon:'fa-shopping-cart',color:'sky',   sub:`${fmt.number(k.total_items_sold)} items`,   badge:`AOV ${fmt.currency(k.avg_order_value)}`, badgeType:'neutral' },
    { label:'Unique Customers',value: fmt.number(k.unique_customers),   icon:'fa-users',       color:'violet',sub:`CLV avg ${fmt.currency(k.avg_clv)}`,         badge:`${fmt.pct(k.return_rate_pct)} returns`, badgeType:'down' },
  ]
  const iconBg = { indigo:'bg-indigo-900/60 text-indigo-400', emerald:'bg-emerald-900/60 text-emerald-400', sky:'bg-sky-900/60 text-sky-400', violet:'bg-violet-900/60 text-violet-400' }
  document.getElementById('kpi-cards').innerHTML = kpiDefs.map(d => `
    <div class="kpi-card fade-in">
      <div class="kpi-icon ${iconBg[d.color]}"><i class="fas ${d.icon}"></i></div>
      <div class="kpi-label">${d.label}</div>
      <div class="kpi-value">${d.value}</div>
      <div class="kpi-sub">${d.sub}</div>
      <div class="kpi-badge ${d.badgeType}">${d.badge}</div>
    </div>`).join('')

  // Annual trend
  const annual = D.annual_revenue
  makeChart('chart-annual', {
    type: 'bar',
    data: {
      labels: annual.map(r => r.year),
      datasets: [
        { label:'Gross Revenue', data: annual.map(r => r.gross_revenue), backgroundColor: 'rgba(99,102,241,0.7)', borderRadius: 4, order: 2 },
        { label:'Net Revenue',   data: annual.map(r => r.net_revenue),   backgroundColor: 'rgba(16,185,129,0.7)', borderRadius: 4, order: 2 },
        { label:'Gross Profit',  data: annual.map(r => r.gross_profit),  type:'line', borderColor:'#f59e0b', backgroundColor:'rgba(245,158,11,0.1)', fill:true, tension:0.4, pointRadius:4, order:1 },
      ],
    },
    options: chartOpts({ prefix:'$', integer:true }),
  })

  // Category bar
  const cats = D.category_revenue.slice(0, 10)
  makeChart('chart-category', {
    type: 'bar',
    data: {
      labels: cats.map(c => c.category),
      datasets: [
        { label:'Revenue',  data: cats.map(c => c.total_revenue), backgroundColor: 'rgba(99,102,241,0.8)', borderRadius: 4 },
        { label:'Profit',   data: cats.map(c => c.total_profit),  backgroundColor: 'rgba(16,185,129,0.8)', borderRadius: 4 },
      ],
    },
    options: { ...chartOpts({ prefix:'$', integer:true }), indexAxis:'y' },
  })
}

// ═══════════════════════════════════════════════════════════════
// REVENUE
// ═══════════════════════════════════════════════════════════════
function buildRevenue() {
  const k = D.kpis
  const revKpis = [
    { label:'Avg Order Value', value: fmt.currency(k.avg_order_value), icon:'fa-receipt',      color:'indigo' },
    { label:'Avg Item Price',  value: fmt.currency(k.avg_item_price),  icon:'fa-tag',           color:'yellow' },
    { label:'Return Rate',     value: fmt.pct(k.return_rate_pct),      icon:'fa-undo',          color:'red'    },
  ]
  const iconBg2 = { indigo:'bg-indigo-900/60 text-indigo-400', yellow:'bg-yellow-900/60 text-yellow-400', red:'bg-red-900/60 text-red-400' }
  document.getElementById('revenue-kpi-cards').innerHTML = revKpis.map(d => `
    <div class="kpi-card fade-in">
      <div class="kpi-icon ${iconBg2[d.color]}"><i class="fas ${d.icon}"></i></div>
      <div class="kpi-label">${d.label}</div>
      <div class="kpi-value">${d.value}</div>
    </div>`).join('')

  // Monthly line chart
  const ms = D.monthly_sales
  makeChart('chart-monthly', {
    type: 'line',
    data: {
      labels: ms.map(r => r.year_month),
      datasets: [
        { label:'Gross Revenue', data: ms.map(r => r.gross_revenue), borderColor:'#6366f1', backgroundColor:'rgba(99,102,241,0.08)', fill:true, tension:0.3, pointRadius:0, pointHoverRadius:4 },
        { label:'Net Revenue',   data: ms.map(r => r.net_revenue),   borderColor:'#10b981', backgroundColor:'rgba(16,185,129,0.08)',  fill:true, tension:0.3, pointRadius:0, pointHoverRadius:4 },
        { label:'Gross Profit',  data: ms.map(r => r.gross_profit),  borderColor:'#f59e0b', backgroundColor:'rgba(245,158,11,0.06)',  fill:false, tension:0.3, pointRadius:0, pointHoverRadius:4, borderDash:[4,3] },
      ],
    },
    options: chartOpts({ prefix:'$', integer:true }),
  })

  // Margin chart
  const cats = D.category_revenue
  makeChart('chart-margin', {
    type: 'bar',
    data: {
      labels: cats.map(c => c.category),
      datasets: [{ label:'Gross Margin %', data: cats.map(c => c.avg_margin_pct), backgroundColor: cats.map(c => c.avg_margin_pct >= 50 ? 'rgba(16,185,129,0.75)' : 'rgba(245,158,11,0.75)'), borderRadius: 4 }],
    },
    options: { ...chartOpts({ suffix:'%' }), indexAxis:'y' },
  })

  // Returns chart
  makeChart('chart-returns', {
    type: 'bar',
    data: {
      labels: cats.map(c => c.category),
      datasets: [{ label:'Return Rate %', data: cats.map(c => c.return_rate_pct), backgroundColor: cats.map(c => c.return_rate_pct >= 13 ? 'rgba(239,68,68,0.75)' : 'rgba(99,102,241,0.65)'), borderRadius: 4 }],
    },
    options: { ...chartOpts({ suffix:'%' }), indexAxis:'y' },
  })
}

// ═══════════════════════════════════════════════════════════════
// CUSTOMERS
// ═══════════════════════════════════════════════════════════════
function buildCustomers() {
  const segs = D.rfm_segments
  const colors = segs.map(s => SEG_COLORS[s.rfm_segment] || '#6366f1')

  makeChart('chart-rfm-pie', {
    type: 'doughnut',
    data: {
      labels: segs.map(s => s.rfm_segment),
      datasets: [{ data: segs.map(s => s.customers), backgroundColor: colors, borderWidth: 0, hoverOffset: 6 }],
    },
    options: {
      plugins: {
        legend: { position:'right', labels:{ color:'#9ca3af', boxWidth:12, padding:14 } },
        tooltip: { callbacks: { label: ctx => ` ${ctx.label}: ${fmt.number(ctx.raw)} customers` } },
      },
      cutout: '60%',
    },
  })

  makeChart('chart-rfm-revenue', {
    type: 'bar',
    data: {
      labels: segs.map(s => s.rfm_segment),
      datasets: [{ label:'Total Revenue', data: segs.map(s => s.total_revenue), backgroundColor: colors, borderRadius: 6 }],
    },
    options: chartOpts({ prefix:'$', integer:true }),
  })

  // Table
  document.getElementById('segments-body').innerHTML = segs.map(s => `
    <tr>
      <td><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${SEG_COLORS[s.rfm_segment] || '#6366f1'};margin-right:8px"></span>${s.rfm_segment}</td>
      <td class="text-right">${fmt.number(s.customers)}</td>
      <td class="text-right">${fmt.currencyK(s.total_revenue)}</td>
      <td class="text-right">${fmt.currency(s.avg_clv)}</td>
      <td class="text-right">${Number(s.avg_orders).toFixed(1)}</td>
      <td class="text-right">${fmt.days(s.avg_recency_days)}</td>
    </tr>`).join('')
}

// ═══════════════════════════════════════════════════════════════
// COHORTS
// ═══════════════════════════════════════════════════════════════
function buildCohorts() {
  const rows = D.cohort_retention

  // Build pivot: cohort_month → { months_since_first → retention_rate }
  const pivot = {}
  rows.forEach(r => {
    if (!pivot[r.cohort_month]) pivot[r.cohort_month] = {}
    pivot[r.cohort_month][r.months_since_first] = r.retention_rate
  })

  const cohorts   = Object.keys(pivot).sort().slice(-18)  // last 18 cohorts
  const maxMonths = 12

  function retColor(v) {
    if (v == null)  return 'background:#111827;color:#374151'
    if (v >= 80)    return 'background:#065f46;color:#6ee7b7'
    if (v >= 60)    return 'background:#064e3b;color:#34d399'
    if (v >= 40)    return 'background:#1e3a2f;color:#10b981'
    if (v >= 20)    return 'background:#1c2b22;color:#6ee7b7'
    if (v >= 10)    return 'background:#1f2937;color:#9ca3af'
    return 'background:#111827;color:#4b5563'
  }

  let html = '<table class="cohort-table"><thead><tr>'
  html += '<th class="text-left pr-4">Cohort</th>'
  html += '<th>Size</th>'
  for (let m = 0; m <= maxMonths; m++) html += `<th>M+${m}</th>`
  html += '</tr></thead><tbody>'

  cohorts.forEach(c => {
    const size = pivot[c][0] || 0
    html += `<tr><td class="text-left pr-4 text-gray-400">${c}</td><td class="text-gray-500">${size}</td>`
    for (let m = 0; m <= maxMonths; m++) {
      const v = pivot[c][m]
      const display = v != null ? v.toFixed(0) + '%' : '—'
      html += `<td style="${retColor(v)};padding:4px 6px;border-radius:3px">${display}</td>`
    }
    html += '</tr>'
  })
  html += '</tbody></table>'
  document.getElementById('cohort-heatmap').innerHTML = html

  // Average retention curve
  const avgByMonth = {}
  const countByMonth = {}
  rows.forEach(r => {
    if (r.months_since_first > maxMonths) return
    avgByMonth[r.months_since_first]   = (avgByMonth[r.months_since_first] || 0) + r.retention_rate
    countByMonth[r.months_since_first] = (countByMonth[r.months_since_first] || 0) + 1
  })
  const months  = Array.from({ length: maxMonths + 1 }, (_, i) => i)
  const avgData = months.map(m => avgByMonth[m] ? +(avgByMonth[m] / countByMonth[m]).toFixed(1) : null)

  makeChart('chart-cohort-avg', {
    type: 'line',
    data: {
      labels: months.map(m => `Month ${m}`),
      datasets: [{
        label: 'Avg Retention %',
        data:  avgData,
        borderColor: '#6366f1',
        backgroundColor: 'rgba(99,102,241,0.1)',
        fill: true, tension: 0.4, pointRadius: 4, pointBackgroundColor: '#6366f1',
      }],
    },
    options: chartOpts({ suffix:'%', max:100 }),
  })
}

// ═══════════════════════════════════════════════════════════════
// PRODUCTS
// ═══════════════════════════════════════════════════════════════
function buildProducts() {
  const prods = D.top_products

  function renderTable(data) {
    document.getElementById('products-body').innerHTML = data.map(p => `
      <tr>
        <td class="font-medium text-gray-200 max-w-xs truncate" title="${p.product_name}">${p.product_name}</td>
        <td>${p.brand || '—'}</td>
        <td><span class="badge-cat">${p.category}</span></td>
        <td class="text-right">${fmt.currency(p.retail_price)}</td>
        <td class="text-right">${fmt.number(p.units_sold)}</td>
        <td class="text-right font-semibold text-indigo-300">${fmt.currencyK(p.total_revenue)}</td>
        <td class="text-right ${Number(p.avg_discount_pct) > 20 ? 'text-yellow-400' : 'text-gray-300'}">${fmt.pct(p.avg_discount_pct)}</td>
        <td class="text-right ${Number(p.return_rate_pct) > 15 ? 'text-red-400' : 'text-gray-300'}">${fmt.pct(p.return_rate_pct)}</td>
      </tr>`).join('')
  }

  renderTable(prods)

  document.getElementById('product-search').addEventListener('input', e => {
    const q = e.target.value.toLowerCase()
    renderTable(prods.filter(p => p.product_name.toLowerCase().includes(q) || (p.brand || '').toLowerCase().includes(q) || p.category.toLowerCase().includes(q)))
  })
}

// ═══════════════════════════════════════════════════════════════
// GEOGRAPHY
// ═══════════════════════════════════════════════════════════════
function buildGeo() {
  const geo = D.geo_revenue
  const clrs = [PALETTE.indigo, PALETTE.emerald, PALETTE.sky, PALETTE.violet, PALETTE.yellow,
                PALETTE.pink, PALETTE.teal, PALETTE.orange, PALETTE.red, PALETTE.lime,
                '#a78bfa','#34d399','#38bdf8','#fb923c','#f472b6']

  makeChart('chart-geo-revenue', {
    type: 'bar',
    data: {
      labels: geo.map(r => r.customer_country),
      datasets: [{ label:'Revenue', data: geo.map(r => r.total_revenue), backgroundColor: clrs, borderRadius: 6 }],
    },
    options: { ...chartOpts({ prefix:'$', integer:true }), indexAxis:'y' },
  })

  makeChart('chart-geo-customers', {
    type: 'doughnut',
    data: {
      labels: geo.map(r => r.customer_country),
      datasets: [{ data: geo.map(r => r.unique_customers), backgroundColor: clrs, borderWidth: 0, hoverOffset: 6 }],
    },
    options: {
      plugins: {
        legend: { position:'right', labels:{ color:'#9ca3af', boxWidth:12, padding:10, font:{ size:11 } } },
        tooltip: { callbacks: { label: ctx => ` ${ctx.label}: ${fmt.number(ctx.raw)} customers` } },
      },
      cutout: '55%',
    },
  })

  document.getElementById('geo-body').innerHTML = geo.map(r => `
    <tr>
      <td class="font-medium text-gray-200">${r.customer_country}</td>
      <td class="text-right">${fmt.number(r.unique_customers)}</td>
      <td class="text-right">${fmt.number(r.total_orders)}</td>
      <td class="text-right font-semibold text-indigo-300">${fmt.currencyK(r.total_revenue)}</td>
      <td class="text-right">${fmt.currency(r.avg_item_price)}</td>
    </tr>`).join('')
}

// ═══════════════════════════════════════════════════════════════
// DATA QUALITY
// ═══════════════════════════════════════════════════════════════
function buildQuality() {
  const q   = D.quality_summary
  const tests = D.quality_tests

  // KPI cards
  const passRate = (q.passed / q.total * 100).toFixed(0)
  document.getElementById('quality-kpi-cards').innerHTML = [
    { label:'Total Tests',      value: q.total,             icon:'fa-vial',          color:'indigo' },
    { label:'Passed',           value: q.passed,            icon:'fa-check-circle',  color:'emerald' },
    { label:'Failed',           value: q.failed,            icon:'fa-times-circle',  color:'red'    },
    { label:'Pass Rate',        value: passRate + '%',      icon:'fa-shield-alt',    color: q.failed === 0 ? 'emerald' : 'yellow' },
  ].map(d => {
    const ibg = { indigo:'bg-indigo-900/60 text-indigo-400', emerald:'bg-emerald-900/60 text-emerald-400', red:'bg-red-900/60 text-red-400', yellow:'bg-yellow-900/60 text-yellow-400' }
    return `<div class="kpi-card fade-in">
      <div class="kpi-icon ${ibg[d.color]}"><i class="fas ${d.icon}"></i></div>
      <div class="kpi-label">${d.label}</div>
      <div class="kpi-value">${d.value}</div>
    </div>`
  }).join('')

  // Category filters
  const categories = [...new Set(tests.map(t => t.category))]
  let activeFilter = null
  const filtersEl = document.getElementById('quality-filters')
  filtersEl.innerHTML = categories.map(c => `<span class="badge-cat" data-cat="${c}">${c}</span>`).join('')

  function renderTests(data) {
    document.getElementById('quality-body').innerHTML = data.map(t => `
      <tr>
        <td class="font-mono text-xs text-gray-300">${t.name}</td>
        <td><span class="badge-cat">${t.category}</span></td>
        <td class="text-gray-400 text-xs">${t.table}</td>
        <td class="text-right"><span class="${t.passed ? 'badge-pass' : 'badge-fail'}">${t.passed ? '✓ PASS' : '✗ FAIL'}</span></td>
        <td class="text-right ${t.failed_rows > 0 ? 'text-red-400' : 'text-gray-400'}">${fmt.number(t.failed_rows)}</td>
        <td class="text-right ${t.failure_rate > 0 ? 'text-red-400' : 'text-gray-400'}">${fmt.pct(t.failure_rate)}</td>
      </tr>`).join('')
  }

  renderTests(tests)

  filtersEl.addEventListener('click', e => {
    const el = e.target.closest('.badge-cat[data-cat]')
    if (!el) return
    const cat = el.dataset.cat
    if (activeFilter === cat) {
      activeFilter = null
      filtersEl.querySelectorAll('.badge-cat').forEach(b => b.classList.remove('active'))
      renderTests(tests)
    } else {
      activeFilter = cat
      filtersEl.querySelectorAll('.badge-cat').forEach(b => b.classList.remove('active'))
      el.classList.add('active')
      renderTests(tests.filter(t => t.category === cat))
    }
  })
}

// ── Shared chart options factory ─────────────────────────────
function chartOpts({ prefix = '', suffix = '', integer = false, max = null } = {}) {
  return {
    responsive: true,
    maintainAspectRatio: true,
    plugins: {
      legend: { labels: { color: '#9ca3af', boxWidth: 12, padding: 16 } },
      tooltip: {
        backgroundColor: '#1f2937',
        borderColor: '#374151',
        borderWidth: 1,
        titleColor: '#f3f4f6',
        bodyColor: '#d1d5db',
        callbacks: {
          label: ctx => {
            const v = ctx.parsed.y ?? ctx.parsed.x ?? ctx.raw
            const n = integer ? Math.round(v).toLocaleString('en-US') : Number(v).toLocaleString('en-US', { maximumFractionDigits: 1 })
            return ` ${ctx.dataset.label}: ${prefix}${n}${suffix}`
          },
        },
      },
    },
    scales: {
      x: { grid: { color: CHART_DEFAULTS.gridColor }, ticks: { color: '#6b7280', maxRotation: 45 } },
      y: {
        grid: { color: CHART_DEFAULTS.gridColor },
        ticks: {
          color: '#6b7280',
          callback: v => prefix + (integer ? Math.round(v).toLocaleString('en-US') : v) + suffix,
        },
        ...(max !== null ? { max } : {}),
      },
    },
  }
}
