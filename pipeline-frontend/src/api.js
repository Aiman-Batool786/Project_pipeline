// Central API client — all calls go through /api which Vite proxies to :8686
const BASE = '/api'

async function request(method, path, body) {
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json' },
  }
  if (body !== undefined) opts.body = JSON.stringify(body)
  const res = await fetch(BASE + path, opts)
  const data = await res.json().catch(() => ({ error: 'Invalid JSON response' }))
  if (!res.ok) throw new Error(data?.detail || data?.error || `HTTP ${res.status}`)
  return data
}

export const api = {
  get:    (path)        => request('GET',    path),
  post:   (path, body)  => request('POST',   path, body),
  delete: (path)        => request('DELETE', path),

  // Health
  health: ()            => api.get('/health'),
  stats:  ()            => api.get('/stats'),

  // Products
  scrapeSearch: (body)  => api.post('/scrape-products', body),
  generateProduct: (body) => api.post('/generate-product', body),
  generateProducts: (body) => api.post('/generate-products', body),
  productInfo: (id)     => api.get(`/product-info/${id}`),

  // Database views
  scrapedProducts: (limit = 100) => api.get(`/scraped-products?limit=${limit}`),
  sellerInfo:      (limit = 100) => api.get(`/seller-info?limit=${limit}`),
  complianceInfo:  (limit = 100) => api.get(`/compliance-info?limit=${limit}`),
  processingLogs:  (limit = 200) => api.get(`/processing-logs?limit=${limit}`),

  // Translation
  translateProduct: (id) => api.post(`/translate-product/${id}`),
  translations: (id)     => api.get(`/translations/${id}`),

  // Variants
  scrapeVariants: (body) => api.post('/scrape-variants', body),
  dbVariants:     (id)   => api.get(`/db/variants/${id}`),
  variantSummary: (id)   => api.get(`/db/variants/${id}/summary`),
  deleteVariants: (id)   => api.delete(`/db/variants/${id}`),

  // Merchant
  submitMerchantIds: (ids) => api.post('/submit-merchant-ids', { merchant_ids: ids }),
  merchantJobStatus: (id)  => api.get(`/merchant-job-status/${id}`),
  merchantJobs: ()          => api.get('/merchant-jobs'),
  merchantStop: (id)        => api.post(`/merchant-stop/${id}`),
  merchantDownload: (id)    => BASE + `/merchant-download/${id}`,

  // Export
  exportTemplates: (onlyNew = false) => api.post(`/export-templates?only_new=${onlyNew}`),

  // Filters
  reloadFilters: () => api.post('/reload-filters'),
}
