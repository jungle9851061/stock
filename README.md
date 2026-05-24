# ETF 成分股即時監控系統

台灣 AI 相關 ETF（00988A、00990A、00981A）成分股即時監控儀表板。後端透過 Yahoo Finance API 取得即時股價，並自動抓取最新成分股清單；前端為單頁 HTML 儀表板，無需框架。

## 功能特色

- **三支 ETF 同時追蹤**：00988A（富邦台灣 AI）、00990A（元大台灣 AI）、00981A（國泰台灣 AI）
- **即時報價**：每 45 秒自動更新，支援台股、日股、韓股、港股、美股、歐股
- **盤前 / 盤後報價**：美股支援延伸交易時段價格顯示
- **成分股變化追蹤**：新進股票標示 `NEW`，權重增減以紅/綠色箭頭顯示
- **ETF 預估淨值**：顯示即時淨值、市價、折溢價
- **全球指數看板**：道瓊、費半、日經、KOSPI、BTC、布蘭特原油、TSM ADR、台指期夜盤
- **市場開盤狀態**：自動偵測各交易所是否開盤中，並優先顯示開盤中市場

## 快速開始

### 安裝相依套件

```bash
pip install flask flask-cors yfinance requests beautifulsoup4 pandas pytz openpyxl
```

### 啟動後端

```bash
python app.py
```

後端預設運行於 `http://127.0.0.1:5000`

### 開啟前端

直接用瀏覽器開啟 `stock.html`（前端會自動呼叫本機 API）

## 專案結構

```
stock/
├── app.py                        # Flask 後端
├── stock.html                    # 單頁前端儀表板
└── holdings/                     # ETF 持股資料（自動產生）
    ├── current_holdings.json     # 00988A 目前成分股
    ├── prev_holdings.json        # 00988A 上次成分股（用於變化比對）
    ├── current_holdings_990.json # 00990A 目前成分股
    ├── prev_holdings_990.json    # 00990A 上次成分股
    ├── current_holdings_981.json # 00981A 目前成分股
    └── prev_holdings_981.json    # 00981A 上次成分股
```

## API 端點

| 端點 | 方法 | 說明 |
|---|---|---|
| `/api/stocks?etf=00988A` | GET | 指定 ETF 成分股即時報價（預設 00988A） |
| `/api/indices` | GET | 全球指數卡片（道瓊、費半、日經等） |
| `/api/reload?etf=00988A` | POST | 從來源重新抓取指定 ETF 成分股 |
| `/api/etf_nav?etf=00988A` | GET | ETF 預估淨值與折溢價 |

## 資料來源

| ETF | 成分股來源 | 淨值來源 |
|---|---|---|
| 00988A | ezmoney XLSX | ezmoney 即時估值 |
| 00990A | 元大投信官網 | 元大 ETF API |
| 00981A | ezmoney XLSX | — |

## 技術說明

### 後端架構

- **雙層快取**：慢層（歷史資料 + metadata，每 10 分鐘刷新）+ 快層（即時報價，每 45 秒刷新）
- **報價 API**：使用 Yahoo Finance v8 chart API，美股/非美股分別處理，避免幣別換算錯誤
- **公假日保護**：非美股市場利用 `history_metadata.regularMarketTime` 偵測節假日，避免誤判開盤

### 前端架構

- 純 Vanilla JS，無框架相依
- 每 20 秒輪詢 `/api/stocks`，每 15 秒輪詢 `/api/indices`
- 支援依地區篩選、開盤市場優先排序

### 股票代號對應

| 後綴 | 交易所 |
|---|---|
| `.TW` | 台灣證交所 |
| `.TWO` | 台灣櫃買中心 |
| `.T` | 東京證交所 |
| `.KS` | 韓國 KOSPI |
| `.KQ` | 韓國 KOSDAQ |
| `.DE` | 德國 Xetra |
| `.PA` | 法國 Euronext |
| `.HK` | 香港聯交所 |
| 無後綴 | 美國市場 |

### 顏色慣例

依台灣股市慣例：**紅色** = 上漲 / 權重增加，**綠色** = 下跌 / 權重減少
