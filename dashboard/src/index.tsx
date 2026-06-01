import { Hono } from 'hono'
import { serveStatic } from 'hono/cloudflare-workers'

const app = new Hono()

// Serve static assets
app.use('/data/*', serveStatic({ root: './' }))
app.use('/static/*', serveStatic({ root: './' }))

// All routes serve the SPA shell
app.get('*', (c) => {
  return c.html(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>TheLook eCommerce — KPI Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fortawesome/fontawesome-free@6.4.0/css/all.min.css" />
  <link rel="stylesheet" href="/static/style.css" />
</head>
<body class="bg-gray-950 text-gray-100 min-h-screen">

  <!-- Sidebar -->
  <nav id="sidebar" class="fixed top-0 left-0 h-full w-64 bg-gray-900 border-r border-gray-800 flex flex-col z-40 transition-transform duration-300">
    <div class="p-5 border-b border-gray-800">
      <div class="flex items-center gap-3">
        <div class="w-9 h-9 rounded-lg bg-indigo-600 flex items-center justify-center">
          <i class="fas fa-chart-line text-white text-sm"></i>
        </div>
        <div>
          <div class="font-bold text-white text-sm">TheLook</div>
          <div class="text-gray-400 text-xs">eCommerce Analytics</div>
        </div>
      </div>
    </div>
    <div class="flex-1 p-3 overflow-y-auto">
      <div class="text-gray-500 text-xs font-semibold uppercase tracking-wider px-2 mb-2 mt-2">Overview</div>
      <a href="#overview"   class="nav-link active" data-section="overview">  <i class="fas fa-th-large w-5"></i> Overview</a>
      <a href="#revenue"    class="nav-link"         data-section="revenue">  <i class="fas fa-dollar-sign w-5"></i> Revenue</a>
      <div class="text-gray-500 text-xs font-semibold uppercase tracking-wider px-2 mb-2 mt-4">Customers</div>
      <a href="#customers"  class="nav-link"         data-section="customers"><i class="fas fa-users w-5"></i> Segments</a>
      <a href="#cohorts"    class="nav-link"         data-section="cohorts">  <i class="fas fa-layer-group w-5"></i> Cohorts</a>
      <div class="text-gray-500 text-xs font-semibold uppercase tracking-wider px-2 mb-2 mt-4">Products</div>
      <a href="#products"   class="nav-link"         data-section="products"> <i class="fas fa-box-open w-5"></i> Products</a>
      <a href="#geo"        class="nav-link"         data-section="geo">      <i class="fas fa-globe w-5"></i> Geography</a>
      <div class="text-gray-500 text-xs font-semibold uppercase tracking-wider px-2 mb-2 mt-4">Pipeline</div>
      <a href="#quality"    class="nav-link"         data-section="quality">  <i class="fas fa-shield-alt w-5"></i> Data Quality</a>
    </div>
    <div class="p-4 border-t border-gray-800">
      <div id="last-updated" class="text-gray-500 text-xs flex items-center gap-2">
        <i class="fas fa-clock"></i> <span>Loading…</span>
      </div>
    </div>
  </nav>

  <!-- Main content -->
  <div class="ml-64 min-h-screen flex flex-col">

    <!-- Topbar -->
    <header class="sticky top-0 z-30 bg-gray-950/80 backdrop-blur border-b border-gray-800 px-8 py-4 flex items-center justify-between">
      <div>
        <h1 id="page-title" class="text-lg font-bold text-white">Overview</h1>
        <p id="page-sub"   class="text-gray-400 text-xs mt-0.5">TheLook eCommerce · All time</p>
      </div>
      <div class="flex items-center gap-3">
        <div id="pipeline-badge" class="hidden items-center gap-2 px-3 py-1.5 rounded-full text-xs font-semibold"></div>
        <div class="text-gray-400 text-sm" id="data-range"></div>
      </div>
    </header>

    <!-- Loading state -->
    <div id="loading" class="flex-1 flex items-center justify-center">
      <div class="text-center">
        <div class="w-12 h-12 border-4 border-indigo-600 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
        <p class="text-gray-400">Loading dashboard data…</p>
      </div>
    </div>

    <!-- Error state -->
    <div id="error-state" class="hidden flex-1 flex items-center justify-center">
      <div class="text-center">
        <i class="fas fa-exclamation-triangle text-yellow-500 text-4xl mb-4"></i>
        <p class="text-gray-300 text-lg font-semibold">Could not load data</p>
        <p class="text-gray-500 text-sm mt-1" id="error-msg"></p>
      </div>
    </div>

    <!-- Dashboard sections -->
    <main id="dashboard" class="hidden flex-1 p-8 space-y-10">

      <!-- ── OVERVIEW ── -->
      <section id="section-overview">
        <div class="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8" id="kpi-cards"></div>
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div class="card">
            <h3 class="card-title"><i class="fas fa-chart-area text-indigo-400 mr-2"></i>Annual Revenue Trend</h3>
            <canvas id="chart-annual" height="220"></canvas>
          </div>
          <div class="card">
            <h3 class="card-title"><i class="fas fa-chart-bar text-emerald-400 mr-2"></i>Revenue by Category (Top 10)</h3>
            <canvas id="chart-category" height="220"></canvas>
          </div>
        </div>
      </section>

      <!-- ── REVENUE ── -->
      <section id="section-revenue" class="hidden">
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-6" id="revenue-kpi-cards"></div>
        <div class="card mb-6">
          <h3 class="card-title"><i class="fas fa-chart-line text-indigo-400 mr-2"></i>Monthly Revenue & Profit (All Years)</h3>
          <canvas id="chart-monthly" height="200"></canvas>
        </div>
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div class="card">
            <h3 class="card-title"><i class="fas fa-percentage text-yellow-400 mr-2"></i>Gross Margin by Category</h3>
            <canvas id="chart-margin" height="220"></canvas>
          </div>
          <div class="card">
            <h3 class="card-title"><i class="fas fa-undo text-red-400 mr-2"></i>Return Rate by Category</h3>
            <canvas id="chart-returns" height="220"></canvas>
          </div>
        </div>
      </section>

      <!-- ── CUSTOMERS ── -->
      <section id="section-customers" class="hidden">
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <div class="card">
            <h3 class="card-title"><i class="fas fa-pie-chart text-indigo-400 mr-2"></i>RFM Customer Segments</h3>
            <canvas id="chart-rfm-pie" height="260"></canvas>
          </div>
          <div class="card">
            <h3 class="card-title"><i class="fas fa-dollar-sign text-emerald-400 mr-2"></i>Revenue by Segment</h3>
            <canvas id="chart-rfm-revenue" height="260"></canvas>
          </div>
        </div>
        <div class="card">
          <h3 class="card-title mb-4"><i class="fas fa-table text-gray-400 mr-2"></i>Segment Detail</h3>
          <div class="overflow-x-auto">
            <table class="w-full text-sm" id="segments-table">
              <thead><tr class="text-gray-400 border-b border-gray-700">
                <th class="text-left pb-3">Segment</th>
                <th class="text-right pb-3">Customers</th>
                <th class="text-right pb-3">Total Revenue</th>
                <th class="text-right pb-3">Avg CLV</th>
                <th class="text-right pb-3">Avg Orders</th>
                <th class="text-right pb-3">Avg Recency</th>
              </tr></thead>
              <tbody id="segments-body"></tbody>
            </table>
          </div>
        </div>
      </section>

      <!-- ── COHORTS ── -->
      <section id="section-cohorts" class="hidden">
        <div class="card mb-6">
          <h3 class="card-title mb-4"><i class="fas fa-layer-group text-indigo-400 mr-2"></i>Cohort Retention Heatmap <span class="text-gray-500 text-xs font-normal">(% retained vs cohort size)</span></h3>
          <div class="overflow-x-auto" id="cohort-heatmap-wrap">
            <div id="cohort-heatmap" class="text-xs"></div>
          </div>
        </div>
        <div class="card">
          <h3 class="card-title"><i class="fas fa-chart-line text-emerald-400 mr-2"></i>Average Retention by Month</h3>
          <canvas id="chart-cohort-avg" height="200"></canvas>
        </div>
      </section>

      <!-- ── PRODUCTS ── -->
      <section id="section-products" class="hidden">
        <div class="card mb-6">
          <div class="flex items-center justify-between mb-4">
            <h3 class="card-title"><i class="fas fa-box-open text-indigo-400 mr-2"></i>Top 20 Products by Revenue</h3>
            <input id="product-search" type="text" placeholder="Search products…"
              class="bg-gray-800 border border-gray-700 rounded-lg px-3 py-1.5 text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-indigo-500 w-52" />
          </div>
          <div class="overflow-x-auto">
            <table class="w-full text-sm">
              <thead><tr class="text-gray-400 border-b border-gray-700">
                <th class="text-left pb-3">Product</th>
                <th class="text-left pb-3">Brand</th>
                <th class="text-left pb-3">Category</th>
                <th class="text-right pb-3">Price</th>
                <th class="text-right pb-3">Units Sold</th>
                <th class="text-right pb-3">Revenue</th>
                <th class="text-right pb-3">Margin</th>
                <th class="text-right pb-3">Return %</th>
              </tr></thead>
              <tbody id="products-body"></tbody>
            </table>
          </div>
        </div>
      </section>

      <!-- ── GEO ── -->
      <section id="section-geo" class="hidden">
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <div class="card">
            <h3 class="card-title"><i class="fas fa-globe text-indigo-400 mr-2"></i>Revenue by Country</h3>
            <canvas id="chart-geo-revenue" height="280"></canvas>
          </div>
          <div class="card">
            <h3 class="card-title"><i class="fas fa-users text-emerald-400 mr-2"></i>Customers by Country</h3>
            <canvas id="chart-geo-customers" height="280"></canvas>
          </div>
        </div>
        <div class="card">
          <h3 class="card-title mb-4"><i class="fas fa-table text-gray-400 mr-2"></i>Country Detail</h3>
          <div class="overflow-x-auto">
            <table class="w-full text-sm">
              <thead><tr class="text-gray-400 border-b border-gray-700">
                <th class="text-left pb-3">Country</th>
                <th class="text-right pb-3">Customers</th>
                <th class="text-right pb-3">Orders</th>
                <th class="text-right pb-3">Revenue</th>
                <th class="text-right pb-3">Avg Price</th>
              </tr></thead>
              <tbody id="geo-body"></tbody>
            </table>
          </div>
        </div>
      </section>

      <!-- ── QUALITY ── -->
      <section id="section-quality" class="hidden">
        <div class="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6" id="quality-kpi-cards"></div>
        <div class="card mb-6">
          <h3 class="card-title mb-4"><i class="fas fa-shield-alt text-indigo-400 mr-2"></i>31 Data Quality Tests</h3>
          <div class="flex flex-wrap gap-2 mb-4" id="quality-filters"></div>
          <div class="overflow-x-auto">
            <table class="w-full text-sm">
              <thead><tr class="text-gray-400 border-b border-gray-700">
                <th class="text-left pb-3">Test Name</th>
                <th class="text-left pb-3">Category</th>
                <th class="text-left pb-3">Table</th>
                <th class="text-right pb-3">Status</th>
                <th class="text-right pb-3">Failed Rows</th>
                <th class="text-right pb-3">Failure Rate</th>
              </tr></thead>
              <tbody id="quality-body"></tbody>
            </table>
          </div>
        </div>
      </section>

    </main>
  </div>

  <script src="/static/app.js"></script>
</body>
</html>`)
})

export default app
