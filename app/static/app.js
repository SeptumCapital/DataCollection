const state = {
  selected: null,
  range: "1y",
  interval: "daily",
  metrics: new Set(["adj_close"]),
  stocks: [],
  view: "dashboard",
  sort: "symbol",
  direction: "asc",
};

const metricLabels = {
  adj_close: "Adjusted Close",
  close: "Close",
  volume: "Volume",
  return_21d: "21D Return",
  rsi_14: "RSI 14",
  sma_50: "SMA 50",
  sma_200: "SMA 200",
  revenue: "Revenue",
  gross_profit: "Gross Profit",
  operating_income: "Operating Income",
  net_income: "Net Income",
  eps_diluted: "Diluted EPS",
  assets: "Assets",
  liabilities: "Liabilities",
  stockholders_equity: "Equity",
  operating_cash_flow: "Operating Cash Flow",
  capex: "Capex",
};

const colors = ["#0f766e", "#1d4ed8", "#b45309", "#7c3aed", "#be123c", "#334155"];
const textSorts = new Set(["symbol", "name", "sector"]);

const $ = (id) => document.getElementById(id);

function formatNumber(value, options = {}) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  const number = Number(value);
  if (options.percent) return `${(number * 100).toFixed(1)}%`;
  if (Math.abs(number) >= 1_000_000_000) return `${(number / 1_000_000_000).toFixed(1)}B`;
  if (Math.abs(number) >= 1_000_000) return `${(number / 1_000_000).toFixed(1)}M`;
  if (Math.abs(number) >= 1_000) return `${(number / 1_000).toFixed(1)}K`;
  return number.toLocaleString(undefined, { maximumFractionDigits: options.digits ?? 2 });
}

function money(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return `$${Number(value).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function percent(value) {
  return formatNumber(value, { percent: true });
}

function analystLabel(score) {
  if (score === null || score === undefined || Number.isNaN(Number(score))) return "-";
  const value = Number(score);
  if (value <= 1.5) return "Strong Buy";
  if (value <= 2.5) return "Buy";
  if (value <= 3.5) return "Hold";
  if (value <= 4.5) return "Sell";
  return "Strong Sell";
}

function signedClass(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "";
  return Number(value) >= 0 ? "pos" : "neg";
}

async function fetchJson(url) {
  const response = await fetch(url);
  const payload = await response.json();
  if (!response.ok || payload.error) throw new Error(payload.error || response.statusText);
  return payload;
}

function queryParams() {
  const params = new URLSearchParams();
  const fields = ["priceMin", "priceMax", "return21Min", "return21Max", "rsiMin", "rsiMax", "instMin", "ratingMax"];
  if ($("searchInput").value.trim()) params.set("q", $("searchInput").value.trim());
  if ($("sectorSelect").value) params.set("sector", $("sectorSelect").value);
  if ($("exchangeSelect").value) params.set("exchange", $("exchangeSelect").value);
  if ($("coverageSelect").value) params.set("hasData", $("coverageSelect").value);
  if ($("conditionSelect").value) params.set("condition", $("conditionSelect").value);
  if ($("insiderBuyOnly").checked) params.set("insiderBuy", "true");
  for (const field of fields) {
    if ($(field).value !== "") params.set(field, $(field).value);
  }
  params.set("sort", state.sort);
  params.set("direction", state.direction);
  params.set("limit", "150");
  return params.toString();
}

async function loadSummary() {
  const summary = await fetchJson("/api/summary");
  $("statConstituents").textContent = summary.constituents;
  $("statPrices").textContent = summary.with_prices;
  $("statTechnicals").textContent = summary.with_technicals;
  $("statFundamentals").textContent = summary.with_fundamentals;
  $("statEnrichment").textContent = summary.with_enrichment;

  $("sectorSelect").innerHTML = `<option value="">All sectors</option>${summary.sectors
    .map((sector) => `<option value="${escapeHtml(sector)}">${escapeHtml(sector)}</option>`)
    .join("")}`;
  $("exchangeSelect").innerHTML = `<option value="">All exchanges</option>${summary.exchanges
    .map((exchange) => `<option value="${escapeHtml(exchange)}">${escapeHtml(exchange)}</option>`)
    .join("")}`;
}

async function loadStocks() {
  const payload = await fetchJson(`/api/stocks?${queryParams()}`);
  state.stocks = payload.rows;
  $("resultCount").textContent = `${payload.total.toLocaleString()} matching stocks`;
  renderRows(payload.rows);
  updateSortControls();
}

function setSort(sortKey, direction = null) {
  if (state.sort === sortKey && direction === null) {
    state.direction = state.direction === "desc" ? "asc" : "desc";
  } else {
    state.sort = sortKey;
    state.direction = direction || (textSorts.has(sortKey) ? "asc" : "desc");
  }
  if ($("sortSelect")) $("sortSelect").value = state.sort;
  loadStocks();
}

function updateSortControls() {
  document.querySelectorAll(".sort-header").forEach((button) => {
    const active = button.dataset.sort === state.sort;
    button.classList.toggle("active", active);
    button.dataset.direction = active ? state.direction : "";
    button.setAttribute(
      "aria-sort",
      active ? (state.direction === "asc" ? "ascending" : "descending") : "none"
    );
  });
}

async function loadTickerTape() {
  const payload = await fetchJson("/api/stocks?sort=symbol&direction=asc&limit=600");
  renderTickerTape(payload.rows || []);
}

async function loadMarketNews() {
  const track = $("marketNewsTrack");
  if (!track) return;
  try {
    const payload = await fetchJson("/api/market-news");
    const items = payload.items || [];
    $("marketNewsSummary").textContent = items.length
      ? `${items.length} headlines from ${payload.provider || "market news"}`
      : payload.error || "No market headlines returned.";
    renderMarketNews(items);
  } catch (error) {
    $("marketNewsSummary").textContent = error.message;
    track.innerHTML = `<div class="market-news-empty">${escapeHtml(error.message)}</div>`;
  }
}

async function loadMomentumRecommendations() {
  const rows = $("momentumRows");
  if (!rows) return;
  rows.innerHTML = '<tr><td colspan="7">Loading momentum recommendations...</td></tr>';
  try {
    const payload = await fetchJson("/api/momentum?limit=10");
    $("momentumSummary").textContent = payload.as_of
      ? `${payload.model} / as of ${shortDateTime(payload.as_of)}`
      : "Cross-sectional S&P 500 leaders";
    renderMomentumRows(payload.rows || []);
  } catch (error) {
    $("momentumSummary").textContent = error.message;
    rows.innerHTML = `<tr><td colspan="8">Momentum unavailable: ${escapeHtml(error.message)}</td></tr>`;
  }
}

async function loadGroupMomentum() {
  const grid = $("groupMomentumGrid");
  if (!grid) return;
  grid.innerHTML = '<div class="market-news-empty">Loading sector and industry momentum...</div>';
  try {
    const payload = await fetchJson("/api/group-momentum?limit=3");
    $("groupMomentumSummary").textContent = payload.as_of
      ? `${payload.model} / as of ${shortDateTime(payload.as_of)}`
      : "Median group returns by period";
    renderGroupMomentum(payload.periods || {});
  } catch (error) {
    $("groupMomentumSummary").textContent = error.message;
    grid.innerHTML = `<div class="market-news-empty">Group momentum unavailable: ${escapeHtml(error.message)}</div>`;
  }
}

function renderGroupMomentum(periods) {
  const grid = $("groupMomentumGrid");
  const labels = ["1W", "1M", "3M", "1Y"];
  grid.innerHTML = labels
    .map((label) => {
      const period = periods[label] || {};
      return `<section class="group-card">
        <h3>${label}</h3>
        <div class="group-list">
          ${groupMomentumList("Sector", period.sectors || [])}
          ${groupMomentumList("Industry", period.industries || [])}
        </div>
      </section>`;
    })
    .join("");
}

function groupMomentumList(title, rows) {
  const items = rows.length
    ? rows
        .map(
          (row) => `<li>
            <span>${escapeHtml(row.name)}</span>
            <strong class="${signedClass(row.momentum)}">${formatNumber(row.momentum, { percent: true })}</strong>
            <small>${formatNumber(row.stock_count, { digits: 0 })} stocks</small>
          </li>`
        )
        .join("")
    : '<li><span>No data</span><strong>-</strong><small></small></li>';
  return `<div class="group-list-block"><h4>${title}</h4><ol>${items}</ol></div>`;
}

function renderMomentumRows(rows) {
  const body = $("momentumRows");
  if (!body) return;
  if (!rows.length) {
    body.innerHTML = '<tr><td colspan="8">No momentum recommendations available.</td></tr>';
    return;
  }
  body.innerHTML = rows
    .map(
      (row) => `<tr tabindex="0" data-symbol="${escapeHtml(row.symbol)}">
        <td>${row.rank}</td>
        <td><strong>${escapeHtml(row.symbol)}</strong><span>${money(row.last_close)}</span></td>
        <td class="momentum-industry"><strong>${escapeHtml(row.sector || "-")}</strong><span>${escapeHtml(row.industry || "-")}</span></td>
        <td class="${signedClass(row.return_1m)}">${formatNumber(row.return_1m, { percent: true })}</td>
        <td class="${signedClass(row.return_3m)}">${formatNumber(row.return_3m, { percent: true })}</td>
        <td class="${signedClass(row.return_12m)}">${formatNumber(row.return_12m, { percent: true })}</td>
        <td class="${signedClass(row.distance_from_sma_200)}">${formatNumber(row.distance_from_sma_200, { percent: true })}</td>
        <td><button class="open-button" type="button" data-symbol="${escapeHtml(row.symbol)}">Open</button></td>
      </tr>`
    )
    .join("");

  body.querySelectorAll("tr").forEach((row) => {
    row.addEventListener("click", () => openDeepDive(row.dataset.symbol));
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter") openDeepDive(row.dataset.symbol);
    });
  });
}

function renderMarketNews(items) {
  const track = $("marketNewsTrack");
  if (!track) return;
  const cleanItems = items.filter((item) => item.title && item.url);
  if (!cleanItems.length) {
    track.innerHTML = '<div class="market-news-empty">No market headlines loaded.</div>';
    return;
  }
  const html = cleanItems.map(marketNewsItemHtml).join("");
  track.innerHTML = `${html}${html}`;
  track.style.setProperty("--news-duration", `${Math.max(42, cleanItems.length * 5)}s`);
}

function marketNewsItemHtml(item) {
  return `<a class="market-news-item" href="${escapeHtml(item.url || "#")}" target="_blank" rel="noreferrer">
    <strong>${escapeHtml(item.title || "Untitled")}</strong>
    <span>${escapeHtml([item.publisher, shortDateTime(item.published_at)].filter(Boolean).join(" / "))}</span>
    <p>${escapeHtml(item.summary || "")}</p>
  </a>`;
}

function renderTickerTape(rows) {
  const track = $("tickerTrack");
  if (!track) return;
  const priced = rows.filter((row) => row.last_close !== null && row.last_close !== undefined);
  if (!priced.length) {
    track.innerHTML = '<span class="ticker-empty">No price data loaded.</span>';
    return;
  }
  const items = priced.map(tickerItemHtml).join("");
  track.innerHTML = `${items}${items}`;
  track.style.setProperty("--ticker-duration", `${Math.max(55, priced.length * 1.15)}s`);
}

function tickerItemHtml(row) {
  const daily = Number(row.return_1d);
  const direction = Number.isFinite(daily) && daily < 0 ? "down" : "up";
  const dailyText = Number.isFinite(daily) ? formatNumber(daily, { percent: true }) : "-";
  return `<button class="ticker-item ${direction}" data-symbol="${escapeHtml(row.symbol)}" type="button">
    <span class="ticker-symbol">${escapeHtml(row.symbol)}</span>
    <span>${money(row.last_close)}</span>
    <span class="ticker-change">${dailyText}</span>
  </button>`;
}

function renderRows(rows) {
  const body = $("stockRows");
  body.innerHTML = rows
    .map(
      (row) => `
      <tr tabindex="0" data-symbol="${escapeHtml(row.symbol)}" class="${row.symbol === state.selected ? "active" : ""}">
        <td class="ticker-symbol">${escapeHtml(row.symbol)}</td>
        <td class="company-cell" title="${escapeHtml(row.name)}">${escapeHtml(row.name)}</td>
        <td>${escapeHtml(row.sector || "-")}</td>
        <td>${money(row.last_close)}</td>
        <td class="${signedClass(row.return_21d)}">${formatNumber(row.return_21d, { percent: true })}</td>
        <td class="${signedClass(row.return_1y)}">${formatNumber(row.return_1y, { percent: true })}</td>
        <td>${formatNumber(row.rsi_14, { digits: 1 })}</td>
        <td>${technicalBadges(row)}</td>
        <td>${row.insider_buy_flag ? '<span class="flag buy">Buy</span>' : '<span class="flag neutral">-</span>'}</td>
        <td>${percent(row.institutions_percent_held)}</td>
        <td>${analystLabel(row.analyst_rating_score)}</td>
        <td>${money(row.price_target_mean)}</td>
        <td>${formatNumber(row.eps_estimate_current_q)}</td>
        <td>${formatNumber(row.revenue_estimate_current_q)}</td>
        <td>${formatNumber(row.volume, { digits: 1 })}</td>
        <td><button class="open-button" type="button" data-symbol="${escapeHtml(row.symbol)}">Open</button></td>
      </tr>`
    )
    .join("");

  body.querySelectorAll("tr").forEach((row) => {
    row.addEventListener("click", () => focusStock(row.dataset.symbol));
    row.querySelector(".open-button")?.addEventListener("click", (event) => {
      event.stopPropagation();
      openDeepDive(event.currentTarget.dataset.symbol);
    });
    row.addEventListener("dblclick", () => openDeepDive(row.dataset.symbol));
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter") openDeepDive(row.dataset.symbol);
    });
  });
}

function attachTickerEvents() {
  const track = $("tickerTrack");
  if (!track) return;
  track.addEventListener("click", (event) => {
    const item = event.target.closest(".ticker-item");
    if (!item) return;
    openDeepDive(item.dataset.symbol);
  });
}

function focusStock(symbol) {
  state.selected = symbol;
  document.querySelectorAll("#stockRows tr").forEach((row) => {
    row.classList.toggle("active", row.dataset.symbol === symbol);
  });
}

function technicalBadges(row) {
  const badges = [];
  if (row.at_52w_high) badges.push('<span class="mini-badge high">52H</span>');
  if (row.at_52w_low) badges.push('<span class="mini-badge low">52L</span>');
  if (row.above_sma_200) badges.push('<span class="mini-badge above">>200</span>');
  if (row.below_sma_200) badges.push('<span class="mini-badge below">&lt;200</span>');
  return badges.join(" ") || '<span class="flag neutral">-</span>';
}

async function selectStock(symbol) {
  focusStock(symbol);
  await Promise.all([loadStockChart(), loadFundamentals(), loadEnrichment(), loadNews(), loadSocial()]);
}

async function loadStockChart() {
  if (!state.selected) return;
  const metrics = Array.from(state.metrics).join(",");
  const payload = await fetchJson(
    `/api/stock/${encodeURIComponent(state.selected)}?range=${state.range}&interval=${state.interval}&metrics=${metrics}`
  );
  const meta = payload.meta || {};
  $("deepSymbol").textContent = payload.symbol;
  $("deepName").textContent = meta.name || payload.symbol;
  $("deepMeta").textContent = [meta.sector, meta.industry, meta.exchange, meta.last_date].filter(Boolean).join(" / ");
  renderSnapshot(meta);
  drawLineChart($("priceChart"), payload.series, payload.metrics, {
    empty: $("chartEmpty"),
    valueFormatter: (value, metric) => (metric.includes("return") ? formatNumber(value, { percent: true }) : formatNumber(value)),
  });
}

function renderSnapshot(meta) {
  const cards = [
    ["Latest Close", money(meta.last_close)],
    ["21D Return", formatNumber(meta.return_21d, { percent: true }), signedClass(meta.return_21d)],
    ["1Y Return", formatNumber(meta.return_1y, { percent: true }), signedClass(meta.return_1y)],
    ["RSI 14", formatNumber(meta.rsi_14, { digits: 1 })],
    ["SMA 200", money(meta.sma_200)],
    ["52W High", money(meta.high_52w)],
    ["52W Low", money(meta.low_52w)],
    ["Technical", technicalBadges(meta)],
    ["Inst Own", percent(meta.institutions_percent_held)],
    ["Insider Buy", meta.insider_buy_flag ? "Yes" : "No", meta.insider_buy_flag ? "pos" : ""],
    ["Rating", analystLabel(meta.analyst_rating_score)],
    ["Target", money(meta.price_target_mean)],
  ];
  $("snapshotGrid").innerHTML = cards
    .map(([label, value, klass]) => `<div class="snapshot-card"><span>${label}</span><strong class="${klass || ""}">${value}</strong></div>`)
    .join("");
}

async function openDeepDive(symbol) {
  state.view = "deep";
  $("dashboardView").classList.add("hidden");
  $("deepDiveView").classList.remove("hidden");
  document.body.classList.add("deep-mode");
  window.scrollTo({ top: 0, behavior: "instant" });
  await selectStock(symbol);
}

function showDashboard() {
  state.view = "dashboard";
  $("deepDiveView").classList.add("hidden");
  $("dashboardView").classList.remove("hidden");
  document.body.classList.remove("deep-mode");
  window.scrollTo({ top: 0, behavior: "instant" });
}

function navigateStock(delta) {
  if (!state.stocks.length || !state.selected) return;
  const current = state.stocks.findIndex((row) => row.symbol === state.selected);
  const next = current < 0 ? 0 : (current + delta + state.stocks.length) % state.stocks.length;
  openDeepDive(state.stocks[next].symbol);
}

async function loadFundamentals() {
  if (!state.selected) return;
  const metric = $("fundamentalMetric").value;
  const payload = await fetchJson(`/api/stock/${encodeURIComponent(state.selected)}/fundamentals?metric=${metric}&form=10-K`);
  drawLineChart($("fundamentalChart"), payload.series.map((row) => ({ date: row.end, [metric]: row.value })), [metric], {
    empty: $("fundamentalEmpty"),
    valueFormatter: (value) => formatNumber(value),
  });
}

async function loadEnrichment() {
  if (!state.selected) return;
  const payload = await fetchJson(`/api/stock/${encodeURIComponent(state.selected)}/enrichment`);
  if (!payload.has_enrichment) {
    $("insiderSummary").textContent = "No ownership or analyst enrichment file for this symbol.";
    $("ownershipSummary").textContent = "No ownership or analyst enrichment file for this symbol.";
    $("analystSummary").textContent = "No ownership or analyst enrichment file for this symbol.";
    $("estimateSummary").textContent = "No ownership or analyst enrichment file for this symbol.";
    $("insiderRows").innerHTML = "";
    $("institutionRows").innerHTML = "";
    $("ratingGrid").innerHTML = "";
    $("estimateRows").innerHTML = "";
    $("insiderFlag").textContent = "-";
    $("insiderFlag").className = "flag neutral";
    return;
  }

  const summary = payload.summary || {};
  $("insiderFlag").textContent = summary.insider_buy_flag ? "Buy Flag" : "No Buy";
  $("insiderFlag").className = summary.insider_buy_flag ? "flag buy" : "flag neutral";
  $("insiderSummary").textContent = `Explicit 6M buys: ${formatNumber(summary.explicit_insider_buy_count, { digits: 0 })} / explicit 6M sells: ${formatNumber(summary.explicit_insider_sell_count, { digits: 0 })}`;
  $("ownershipSummary").textContent = `Institutions hold ${percent(summary.institutions_percent_held)} / insiders hold ${percent(summary.insiders_percent_held)}`;
  $("analystSummary").textContent = `${analystLabel(summary.analyst_rating_score)} from ${formatNumber(summary.analyst_rating_count, { digits: 0 })} ratings`;
  $("estimateSummary").textContent = `Current quarter EPS avg ${formatNumber(summary.eps_estimate_current_q)} / revenue avg ${formatNumber(summary.revenue_estimate_current_q)}`;

  $("insiderRows").innerHTML = (payload.insider_transactions || [])
    .slice(0, 40)
    .map((row) => {
      return `<tr>
        <td>${escapeHtml(row["Start Date"] || row["Transaction Start Date"] || "-")}</td>
        <td class="company-cell" title="${escapeHtml(row["Insider"] || row["Insider Name"] || "")}">${escapeHtml(row["Insider"] || row["Insider Name"] || "-")}</td>
        <td><span class="trade-action ${row.action === "Buy" ? "buy-text" : "sell-text"}">${escapeHtml(row.action || "-")}</span></td>
        <td>${formatNumber(row["Shares"], { digits: 0 })}</td>
        <td>${money(row["Value"])}</td>
      </tr>`;
    })
    .join("");

  $("institutionRows").innerHTML = (payload.institutional_holders || [])
    .slice(0, 12)
    .map(
      (row) => `<tr>
        <td class="company-cell" title="${escapeHtml(row.Holder || "")}">${escapeHtml(row.Holder || "-")}</td>
        <td>${formatNumber(row.Shares, { digits: 0 })}</td>
        <td>${money(row.Value)}</td>
        <td>${percent(row.pctHeld)}</td>
      </tr>`
    )
    .join("");

  const targets = payload.analyst_price_targets || {};
  $("ratingGrid").innerHTML = [
    ["Rating", analystLabel(summary.analyst_rating_score)],
    ["Score", formatNumber(summary.analyst_rating_score, { digits: 2 })],
    ["Mean Target", money(targets.mean)],
    ["High Target", money(targets.high)],
    ["Low Target", money(targets.low)],
    ["Ratings", formatNumber(summary.analyst_rating_count, { digits: 0 })],
  ]
    .map(([label, value]) => `<div class="rating-box"><span>${label}</span><strong>${value}</strong></div>`)
    .join("");

  const revenueByPeriod = new Map((payload.revenue_estimate || []).map((row) => [row.period, row]));
  $("estimateRows").innerHTML = (payload.earnings_estimate || [])
    .slice(0, 8)
    .map((row) => {
      const revenue = revenueByPeriod.get(row.period) || {};
      return `<tr>
        <td>${escapeHtml(row.period || "-")}</td>
        <td>${formatNumber(row.avg)}</td>
        <td>${formatNumber(revenue.avg)}</td>
        <td>${formatNumber(row.numberOfAnalysts || revenue.numberOfAnalysts, { digits: 0 })}</td>
      </tr>`;
    })
    .join("");
}

async function loadNews() {
  if (!state.selected) return;
  const payload = await fetchJson(`/api/stock/${encodeURIComponent(state.selected)}/news`);
  const items = payload.items || [];
  $("newsSummary").textContent = items.length
    ? `${items.length} recent headlines from ${payload.provider || "news provider"}`
    : payload.error || "No recent news returned.";
  $("newsList").innerHTML = items
    .slice(0, 8)
    .map(
      (item) => `<a class="news-item" href="${escapeHtml(item.url || "#")}" target="_blank" rel="noreferrer">
        <strong>${escapeHtml(item.title || "Untitled")}</strong>
        <span>${escapeHtml([item.publisher, shortDateTime(item.published_at)].filter(Boolean).join(" / "))}</span>
        <p>${escapeHtml(item.summary || "")}</p>
      </a>`
    )
    .join("");
}

async function loadSocial() {
  if (!state.selected) return;
  const payload = await fetchJson(`/api/stock/${encodeURIComponent(state.selected)}/social`);
  const posts = payload.posts || [];
  $("socialSummary").textContent = posts.length
    ? `${posts.length} popular posts from ${payload.provider || "local social data"}`
    : payload.message || "No popular X posts loaded.";
  $("socialList").innerHTML = posts
    .slice(0, 8)
    .map((post) => {
      const url = post.url || post.link || "#";
      const author = post.author || post.username || post.handle || "X";
      const metric = post.likes || post.like_count || post.retweets || post.repost_count || "";
      return `<a class="news-item" href="${escapeHtml(url)}" target="_blank" rel="noreferrer">
        <strong>${escapeHtml(author)}</strong>
        <span>${escapeHtml([shortDateTime(post.created_at || post.date), metric ? `${formatNumber(metric, { digits: 0 })} interactions` : ""].filter(Boolean).join(" / "))}</span>
        <p>${escapeHtml(post.text || post.content || "")}</p>
      </a>`;
    })
    .join("");
}

function drawLineChart(canvas, rows, metrics, options = {}) {
  const activeMetrics = metrics.filter((metric) => rows.some((row) => row[metric] !== null && row[metric] !== undefined));
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  const tooltip = ensureChartTooltip(canvas);
  if (!rows.length || !activeMetrics.length) {
    ctx.clearRect(0, 0, width, height);
    tooltip.classList.remove("visible");
    options.empty?.classList.add("visible");
    canvas._chartState = null;
    return;
  }
  options.empty?.classList.remove("visible");

  const pad = { left: 66, right: 24, top: 22, bottom: 42 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;
  const xs = rows.map((row) => new Date(row.date).getTime());
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);

  const values = [];
  for (const metric of activeMetrics) {
    for (const row of rows) {
      const value = Number(row[metric]);
      if (!Number.isNaN(value) && Number.isFinite(value)) values.push(value);
    }
  }
  let minY = Math.min(...values);
  let maxY = Math.max(...values);
  if (minY === maxY) {
    minY -= 1;
    maxY += 1;
  }
  const yPad = (maxY - minY) * 0.08;
  minY -= yPad;
  maxY += yPad;

  const xFor = (time) => pad.left + ((time - minX) / Math.max(1, maxX - minX)) * plotW;
  const yFor = (value) => pad.top + (1 - (value - minY) / (maxY - minY)) * plotH;

  const points = rows.map((row, rowIndex) => {
    const valuesForRow = activeMetrics
      .map((metric, metricIndex) => {
        const value = Number(row[metric]);
        if (Number.isNaN(value) || !Number.isFinite(value)) return null;
        return {
          metric,
          metricIndex,
          value,
          x: xFor(new Date(row.date).getTime()),
          y: yFor(value),
        };
      })
      .filter(Boolean);
    return {
      row,
      rowIndex,
      date: row.date,
      x: xFor(new Date(row.date).getTime()),
      values: valuesForRow,
    };
  });

  const chartState = {
    canvas,
    ctx,
    tooltip,
    width,
    height,
    rows,
    activeMetrics,
    options,
    pad,
    plotW,
    plotH,
    minY,
    maxY,
    xFor,
    yFor,
    points,
    pinned: null,
    hover: null,
  };
  canvas._chartState = chartState;
  bindChartPointerEvents(canvas);
  renderLineChart(chartState);
}

function renderLineChart(chartState) {
  const { canvas, ctx, width, height, rows, activeMetrics, options, pad, plotW, plotH, minY, maxY, xFor, yFor } = chartState;
  ctx.clearRect(0, 0, width, height);

  ctx.strokeStyle = "#e2e8f0";
  ctx.lineWidth = 1;
  ctx.fillStyle = "#64748b";
  ctx.font = "12px system-ui";
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";

  for (let i = 0; i <= 4; i += 1) {
    const y = pad.top + (plotH / 4) * i;
    const value = maxY - ((maxY - minY) / 4) * i;
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(width - pad.right, y);
    ctx.stroke();
    ctx.fillText(options.valueFormatter ? options.valueFormatter(value, activeMetrics[0]) : formatNumber(value), pad.left - 10, y);
  }

  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  const tickCount = Math.min(5, rows.length);
  for (let i = 0; i < tickCount; i += 1) {
    const index = Math.round((rows.length - 1) * (i / Math.max(1, tickCount - 1)));
    const row = rows[index];
    ctx.fillText(shortDate(row.date), xFor(new Date(row.date).getTime()), height - pad.bottom + 16);
  }

  activeMetrics.forEach((metric, metricIndex) => {
    ctx.strokeStyle = colors[metricIndex % colors.length];
    ctx.lineWidth = 2;
    ctx.beginPath();
    let started = false;
    rows.forEach((row) => {
      const value = Number(row[metric]);
      if (Number.isNaN(value) || !Number.isFinite(value)) return;
      const x = xFor(new Date(row.date).getTime());
      const y = yFor(value);
      if (!started) {
        ctx.moveTo(x, y);
        started = true;
      } else {
        ctx.lineTo(x, y);
      }
    });
    ctx.stroke();
  });

  ctx.textAlign = "left";
  ctx.textBaseline = "top";
  activeMetrics.forEach((metric, index) => {
    const x = pad.left + index * 128;
    ctx.fillStyle = colors[index % colors.length];
    ctx.fillRect(x, 8, 10, 10);
    ctx.fillStyle = "#334155";
    ctx.fillText(metricLabels[metric] || metric, x + 15, 5);
  });

  const selection = chartState.pinned || chartState.hover;
  if (selection) drawChartSelection(chartState, selection);
}

function drawChartSelection(chartState, selection) {
  const { ctx, width, height, pad, tooltip, options } = chartState;
  const point = chartState.points[selection.index];
  if (!point || !point.values.length) return;

  ctx.save();
  ctx.strokeStyle = "#0f172a";
  ctx.lineWidth = 1;
  ctx.setLineDash([4, 4]);
  ctx.beginPath();
  ctx.moveTo(point.x, pad.top);
  ctx.lineTo(point.x, height - pad.bottom);
  ctx.stroke();
  ctx.setLineDash([]);

  point.values.forEach((entry) => {
    ctx.fillStyle = colors[entry.metricIndex % colors.length];
    ctx.beginPath();
    ctx.arc(entry.x, entry.y, 4, 0, Math.PI * 2);
    ctx.fill();
    ctx.strokeStyle = "#ffffff";
    ctx.lineWidth = 2;
    ctx.stroke();
  });
  ctx.restore();

  const displayRows = point.values
    .map((entry) => {
      const label = metricLabels[entry.metric] || entry.metric;
      const value = options.valueFormatter ? options.valueFormatter(entry.value, entry.metric) : formatNumber(entry.value);
      return `<div><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;
    })
    .join("");
  tooltip.innerHTML = `<header>${escapeHtml(longDate(point.date))}</header>${displayRows}`;

  const rect = chartCanvasRect(chartState.canvas);
  const cssX = point.x * rect.scaleX;
  const cssY = Math.min(...point.values.map((entry) => entry.y)) * rect.scaleY;
  const tooltipWidth = 190;
  const left = Math.min(Math.max(10, cssX + 12), rect.width - tooltipWidth - 10);
  const top = Math.min(Math.max(10, cssY + 12), rect.height - 120);
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
  tooltip.classList.add("visible");
}

function ensureChartTooltip(canvas) {
  const parent = canvas.parentElement;
  let tooltip = parent.querySelector(":scope > .chart-tooltip");
  if (!tooltip) {
    tooltip = document.createElement("div");
    tooltip.className = "chart-tooltip";
    parent.appendChild(tooltip);
  }
  return tooltip;
}

function bindChartPointerEvents(canvas) {
  if (canvas._chartPointerBound) return;
  canvas._chartPointerBound = true;
  canvas.addEventListener("pointermove", (event) => {
    const stateForCanvas = canvas._chartState;
    if (!stateForCanvas || stateForCanvas.pinned) return;
    stateForCanvas.hover = nearestChartPoint(stateForCanvas, event);
    renderLineChart(stateForCanvas);
  });
  canvas.addEventListener("pointerleave", () => {
    const stateForCanvas = canvas._chartState;
    if (!stateForCanvas || stateForCanvas.pinned) return;
    stateForCanvas.hover = null;
    stateForCanvas.tooltip.classList.remove("visible");
    renderLineChart(stateForCanvas);
  });
  canvas.addEventListener("click", (event) => {
    const stateForCanvas = canvas._chartState;
    if (!stateForCanvas) return;
    stateForCanvas.pinned = nearestChartPoint(stateForCanvas, event);
    stateForCanvas.hover = null;
    renderLineChart(stateForCanvas);
  });
  canvas.addEventListener("dblclick", () => {
    const stateForCanvas = canvas._chartState;
    if (!stateForCanvas) return;
    stateForCanvas.pinned = null;
    stateForCanvas.hover = null;
    stateForCanvas.tooltip.classList.remove("visible");
    renderLineChart(stateForCanvas);
  });
}

function chartCanvasRect(canvas) {
  const rect = canvas.getBoundingClientRect();
  return {
    width: rect.width,
    height: rect.height,
    scaleX: rect.width / canvas.width,
    scaleY: rect.height / canvas.height,
  };
}

function nearestChartPoint(chartState, event) {
  const rect = chartState.canvas.getBoundingClientRect();
  const scale = chartState.canvas.width / rect.width;
  const x = (event.clientX - rect.left) * scale;
  let best = { index: 0, distance: Infinity };
  chartState.points.forEach((point, index) => {
    if (!point.values.length) return;
    const distance = Math.abs(point.x - x);
    if (distance < best.distance) best = { index, distance };
  });
  return best.distance === Infinity ? null : { index: best.index };
}

function shortDate(value) {
  const date = parseDisplayDate(value);
  return date.toLocaleDateString(undefined, { month: "short", year: "2-digit" });
}

function longDate(value) {
  const date = parseDisplayDate(value);
  if (Number.isNaN(date.getTime())) return value || "";
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function parseDisplayDate(value) {
  const match = String(value || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (match) return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  return new Date(value);
}

function shortDateTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function attachEvents() {
  const filterIds = [
    "searchInput",
    "sectorSelect",
    "exchangeSelect",
    "coverageSelect",
    "conditionSelect",
    "priceMin",
    "priceMax",
    "return21Min",
    "return21Max",
    "rsiMin",
    "rsiMax",
    "instMin",
    "ratingMax",
  ];
  let timer = null;
  filterIds.forEach((id) => {
    $(id).addEventListener("input", () => {
      clearTimeout(timer);
      timer = setTimeout(loadStocks, 160);
    });
    $(id).addEventListener("change", loadStocks);
  });

  $("resetButton").addEventListener("click", () => {
    filterIds.forEach((id) => {
      if (id !== "sortSelect") $(id).value = "";
    });
    $("sortSelect").value = "symbol";
    loadStocks();
  });
  $("refreshButton").addEventListener("click", loadStocks);
  $("sortSelect").addEventListener("change", (event) => {
    setSort(event.target.value, textSorts.has(event.target.value) ? "asc" : "desc");
  });
  document.querySelectorAll(".sort-header").forEach((button) => {
    button.addEventListener("click", () => setSort(button.dataset.sort));
  });
  $("insiderBuyOnly").addEventListener("change", loadStocks);
  $("fundamentalMetric").addEventListener("change", loadFundamentals);
  $("backButton").addEventListener("click", showDashboard);
  $("prevStockButton").addEventListener("click", () => navigateStock(-1));
  $("nextStockButton").addEventListener("click", () => navigateStock(1));

  $("rangeButtons").querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => {
      state.range = button.dataset.range;
      setActive("rangeButtons", button);
      loadStockChart();
    });
  });
  $("intervalButtons").querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => {
      state.interval = button.dataset.interval;
      setActive("intervalButtons", button);
      loadStockChart();
    });
  });
  $("metricButtons").querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => {
      const metric = button.dataset.metric;
      if (state.metrics.has(metric)) {
        if (state.metrics.size > 1) state.metrics.delete(metric);
      } else {
        state.metrics.add(metric);
      }
      button.classList.toggle("active", state.metrics.has(metric));
      loadStockChart();
    });
  });
}

function setActive(groupId, button) {
  $(groupId).querySelectorAll("button").forEach((candidate) => candidate.classList.remove("active"));
  button.classList.add("active");
}

async function init() {
  attachEvents();
  attachTickerEvents();
  await loadSummary();
  await loadStocks();
  loadTickerTape().catch((error) => console.error("Ticker tape failed", error));
  loadMomentumRecommendations().catch((error) => console.error("Momentum recommendations failed", error));
  loadGroupMomentum().catch((error) => console.error("Group momentum failed", error));
  loadMarketNews().catch((error) => console.error("Market news failed", error));
}

init().catch((error) => {
  $("resultCount").textContent = error.message;
  console.error(error);
});
