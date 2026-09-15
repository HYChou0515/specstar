# 貢獻指南

感謝你對 SpecStar 的關注！我們歡迎各種形式的貢獻。

## 貢獻方式

### 回報問題

如果你發現 bug 或有功能建議，請在 [GitHub Issues](https://github.com/HYChou0515/specstar/issues) 提出。

提交 issue 時請包含：

- **問題描述**：清楚描述問題或建議
- **重現步驟**：如何重現問題（若是 bug）
- **預期行為**：你期望發生什麼
- **實際行為**：實際發生了什麼
- **環境資訊**：Python 版本、SpecStar 版本、作業系統

### 提交程式碼

1. **Fork 專案**

   Fork [specstar](https://github.com/HYChou0515/specstar) 到你的 GitHub 帳號

2. **Clone 到本地**

   ```bash
   git clone https://github.com/YOUR_USERNAME/specstar.git
   cd specstar
   ```

3. **安裝開發環境**

   ```bash
   # 使用 uv 安裝依賴
   uv sync
   
   # 或使用 pip
   pip install -e ".[dev]"
   ```

4. **創建分支**

   ```bash
   git checkout -b feature/your-feature-name
   ```

5. **開發與測試**

   ```bash
   # 自動格式化程式碼
   make style
   
   # 執行測試
   make test
   ```

6. **提交變更**

   ```bash
   git add .
   git commit -m "描述你的變更"
   ```

7. **推送到 GitHub**

   ```bash
   git push origin feature/your-feature-name
   ```

8. **創建 Pull Request**

   在 GitHub 上創建 Pull Request，描述你的變更內容

## 開發規範

### 程式碼風格

- 使用 **ruff** 進行程式碼格式化和檢查
- 執行 `make style` 自動格式化程式碼
- 執行 `make check` 檢查程式碼品質

### 資料模型

- **必須使用 `msgspec.Struct`**，不使用 Pydantic
- 所有欄位都需要類型標註
- 範例：

  ```python
  from msgspec import Struct
  
  class User(Struct):
      name: str
      age: int
      email: str | None = None
  ```

### 測試要求

- **所有新功能必須包含測試**
- 目標：**90% 以上的程式碼覆蓋率**
- 測試檔案放在 `tests/` 目錄
- 執行測試：`make test`
- 查看覆蓋率：`make test`

### Commit 訊息

使用清楚的 commit 訊息：

```
feat: 新增 XXX 功能
fix: 修復 XXX 問題
docs: 更新文檔
test: 新增測試
refactor: 重構程式碼
perf: 效能優化
```

### 文檔

- 更新相關文檔（`docs/` 目錄）
- 為新功能添加範例
- 更新 API 參考文檔

### 變更紀錄（Changelog）

`CHANGELOG.md` 的新版本區段由 [git-cliff](https://git-cliff.org/) 從
**Conventional Commits** 自動生成,**不要直接手改尚未釋出的區段**(手寫的
歷史區段 ≤ 0.11.1 會保留不動)。

因此面向使用者的變更請用規範化的 commit 訊息,它才會進 changelog:

```
feat: 新增 XXX          → Added
fix: 修復 XXX           → Fixed
perf: 效能優化          → Performance
refactor: 重構 XXX      → Changed
docs: 文件             → Documentation
```

> **注意**:不符合格式的 commit(例如 `--`、`wip`)以及 `ci`/`chore`/`test`/
> 合併 commit **會被忽略,不會出現在 changelog**。真正面向使用者的變更請務必
> 用上面的前綴。

```bash
make changelog-preview          # 預覽尚未釋出的紀錄(從 commit 生成)
```

釋出時由維護者執行 `make release patch|minor|major`(版本由 git-cliff 依語意化
規則自動算出),或 `make release VERSION=X.Y.Z` 指定明確版本。它會:bump
`__version__` → 把尚未釋出的 commit 收成 `## [X.Y.Z]` 區段(插到既有歷史之前)
→ commit `bump vX.Y.Z`。接著 `make release-publish` 打上 `vX.Y.Z` tag 並
push,由 CI 完成上傳(見「發布流程」)。

## 開發工作流程

### 本地測試

```bash
# 快速開發循環（格式化 + 測試）
make dev

# 完整 CI 流程（檢查 + 測試 + 覆蓋率）
make ci

# 查看 HTML 覆蓋率報告
make cov-html
# 然後開啟 htmlcov/index.html
```

### 文檔預覽

```bash
# 啟動文檔伺服器
make serve

# 訪問 http://localhost:8000
```

### 效能測試

```bash
# 執行基準測試
make test-benchmark
```

## 專案結構

```
specstar/
├── specstar/           # 核心程式碼
│   ├── crud/          # SpecStar 主要邏輯
│   ├── resource_manager/  # 資源管理
│   ├── permission/    # 權限系統
│   ├── message_queue/ # 訊息佇列
│   └── util/          # 工具函數
├── tests/             # 測試檔案
├── examples/          # 範例程式
├── docs/              # 文檔
│   ├── en/           # 英文文檔
│   └── zh/           # 中文文檔
└── scripts/           # 工具腳本
```

## 貢獻範例

### 新增功能範例

如果你實作了一個有趣的應用範例：

1. 在 `examples/` 目錄下創建新檔案
2. 確保範例可以執行
3. 在 `docs/zh/examples/index.md` 添加說明
4. 提交 Pull Request

範例應該：

- 使用 `msgspec.Struct` 定義模型
- 包含完整的註解
- 可以直接執行
- 展示特定功能或應用場景

### 報告 Bug

提交 bug 報告時，請使用以下範本：

```markdown
**問題描述**
簡短描述問題

**重現步驟**
1. 執行 '...'
2. 訪問 '...'
3. 觀察錯誤

**預期行為**
應該要...

**實際行為**
實際上...

**環境**
- OS: [e.g. Ubuntu 22.04]
- Python: [e.g. 3.11.5]
- SpecStar: [e.g. 0.1.0]

**額外資訊**
其他相關資訊、截圖、錯誤訊息等
```

## 發布流程

（僅供維護者參考）

**推 tag 就是發布動作。** 上傳一律由 GitHub Actions 執行，用 OIDC trusted
publishing，本機不再持有 PyPI／npm 的長期 token，也沒有第二條上傳路徑。

本專案有**兩個各自獨立**的產出與版號流：

| tag | 版號來源 | 版號規則 | registry | workflow |
| --- | --- | --- | --- | --- |
| `vX.Y.Z` | `specstar/__init__.py` | PEP 440 | PyPI | `release-pypi.yml` |
| `web-vX.Y.Z` | `web/generator/package.json` | SemVer | npm | `release-npm.yml` |

兩者刻意不統一版號：PEP 440 的預發行版寫作 `0.13.0a2`、SemVer 寫作
`0.13.0-alpha.2`，互不相通（`scripts/next_version.py` 的註解記著上次硬要
互通的後果）。

### Python 套件 → PyPI

```bash
make release alpha|beta|rc|final|patch|minor|major   # bump + CHANGELOG + commit
# 或 make release VERSION=X.Y.Z
make release-publish                                  # build/twine check(pre-flight) + 打 tag + push
```

`release-publish` 只負責推 tag；CI 收到 tag 後會**重新驗證 tag 與
`__version__` 一致**（`scripts/release_tag.py`）→ build → 上傳。

### npm generator → npm

```bash
make -C web sync-templates      # 若改過 web/app 才需要，改完要 commit
make -C web release             # 打 web-vX.Y.Z tag + push
```

CI 會先擋兩件事才發布：tag 與 `package.json` 版號必須一致，且
`generator/templates/base` 必須已經跟 `web/app` 同步
（`make -C web check-templates`）—— 本機舊流程會在打包前自動 sync，CI 只發
tag 指到的 commit，所以這道檢查是把那個步驟換成一個會紅的守衛。預發行版
（`0.4.0-rc.1`）會發到 npm 的 `next` dist-tag，不會搶走 `latest`。

### 一次性設定

**PyPI** —— 在專案設定新增 Trusted Publisher（repo `HYChou0515/specstar`、
workflow `release-pypi.yml`、environment `pypi`）。

**npm** —— 兩步，且順序不能反。

1. npm **不能用 OIDC 發套件的第一版**：trusted publisher 要在套件設定頁設
   定，而設定頁得等套件存在（npm/cli#8544）。`0.3.4` 已於 2026-07-28 用
   `make -C web bootstrap-publish` 手動發過，這一步**不需要再做**。
2. 註冊 trusted publisher。用 CLI 就好，不必開網頁：

   ```bash
   npx -y npm@11 trust github specstar-web-generator \
     --file release-npm.yml --repo HYChou0515/specstar \
     --env npm --allow-publish
   ```

   npm 規定 trust 操作必須通過**帳號層級 2FA**，並且明文不接受
   bypass-2FA 的 granular token —— 所以這行只能由維護者本人跑，會跳出
   one-time password 提示。先 `--dry-run` 可以確認參數。

### 排查：npm 發布失敗

npm 把**所有**未通過認證的 publish 都回報成 `E404 Not Found` 或
`ENEEDAUTH`（npm/cli#9088）。看到 404 不要去查「套件不存在」或權限，那是
假線索 —— 它的意思是「npm 不接受你的身分」。依序看：

- workflow 的 **OIDC preconditions** 步驟有沒有印出 `id-token permission:
  GRANTED`。沒有 → job 少了 `permissions: id-token: write`。
- **No stored credentials may pre-empt OIDC** 有沒有紅。紅了表示有東西把
  憑證寫進 `.npmrc`，npm 會**優先用它而完全不去做 OIDC 交換**。最常見的兇
  手是 `actions/setup-node` 的 `registry-url:` —— 它會寫入
  `//registry.npmjs.org/:_authToken=${NODE_AUTH_TOKEN}` 並把
  `NODE_AUTH_TOKEN` 預設成字面字串 `XXXXX-XXXXX-XXXXX-XXXXX`
  （actions/setup-node#1551）。workflow 裡那道 `sed '/_authToken/d'` 就是
  為此存在，**不要拿掉**。
- 以上都綠但仍 `ENEEDAUTH` → npmjs.com 上沒有相符的 trusted publisher，回
  上面第 2 步。三元組（repository / workflow 檔名 / environment）必須逐字
  相同，workflow 只填檔名不含路徑。

## 授權

提交貢獻即表示你同意將你的貢獻以與本專案相同的授權條款（MIT License）釋出。

## 需要幫助？

- 查看 [文檔](https://hychou0515.github.io/specstar/)
- 提出 [Issue](https://github.com/HYChou0515/specstar/issues)
- 查看現有的 [Pull Requests](https://github.com/HYChou0515/specstar/pulls)

感謝你的貢獻！🚀
