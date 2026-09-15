# SpecStar Web - Makefile
# 常用開發指令

# Configurable backend API URL (override with: make gen-app API_URL=http://myhost:9000)
# Used to fetch OpenAPI spec during generation and as Vite dev proxy target
API_URL ?= http://localhost:8000
BASE_PATH ?= /v1/specstar

# Include test files in generated app (override with: make gen-app INCLUDE_TESTS=1)
INCLUDE_TESTS ?=
ifneq ($(INCLUDE_TESTS),)
  INIT_TEST_FLAG := --include-tests
else
  INIT_TEST_FLAG :=
endif

.PHONY: help build-gen gen dev dev-app dev-gen install clean test clean-app gen-app reset-app regen-app style

# 預設目標：顯示幫助
help:
	@echo "SpecStar Web - 常用指令"
	@echo ""
	@echo "App 管理（推薦）："
	@echo "  make clean-app    - 刪除整個 app 目錄"
	@echo "  make gen-app      - 用 generator 生成全新的 app（需先 build-gen）"
	@echo "  make reset-app    - 刪除 + 重新生成 app（完整重置）"
	@echo "  make regen-app    - 從 SpecStar 後端重新生成前端代碼"
	@echo "  make dev-app      - 啟動 app 開發服務器"
	@echo ""
	@echo "開發流程："
	@echo "  make install      - 安裝所有依賴"
	@echo "  make build-gen    - 編譯 generator"
	@echo "  make gen          - 重新生成前端代碼"
	@echo "  make dev          - 啟動開發服務器"
	@echo "  make style        - 自動修復 lint + 格式化（commit 前執行）"
	@echo ""
	@echo "組合指令："
	@echo "  make rebuild      - 重新編譯 generator + 重新生成代碼"
	@echo "  make reset        - 清理 + 安裝 + 編譯 + 生成"
	@echo ""
	@echo "釋出："
	@echo "  make sync-templates    - 把 app/ 同步進 generator/templates/base"
	@echo "  make check-templates   - 驗證 templates 已同步（不寫入）"
	@echo "  make release           - 打 web-vX.Y.Z tag + push；上傳由 CI 執行"
	@echo "  make trust             - 一次性：註冊 npm trusted publisher（要 2FA）"
	@echo "  make bootstrap-publish - 一次性：首發到 npm（已完成，勿再用）"
	@echo ""
	@echo "其他："
	@echo "  make clean        - 清理生成的檔案"
	@echo "  make test         - 執行測試"

# 安裝依賴
install:
	@echo "📦 安裝 generator 依賴..."
	cd generator && pnpm install
	@echo "📦 安裝 app 依賴..."
	cd app && pnpm install
	@echo "✅ 安裝完成"

# 編譯 generator
build-gen:
	@echo "🔨 編譯 generator..."
	cd generator && pnpm build
	@echo "✅ Generator 編譯完成"

# 重新生成前端代碼
gen:
	@echo "🚀 重新生成前端代碼..."
	cd app && rm -rf src/specstar/generated
	cd app && node ../generator/dist/cli.js generate
	@echo "✅ 代碼生成完成"

# 啟動開發服務器
dev:
	@echo "🚀 啟動開發服務器 (port 5175)..."
	cd app && pnpm dev --port 5175

# 啟動開發服務器（預設端口）
dev-app:
	@echo "🚀 啟動 app 開發服務器..."
	cd app && pnpm dev

# 組合：重新編譯 + 重新生成
rebuild: build-gen gen
	@echo "✅ 重新編譯和生成完成"

# 組合：完整重置
reset: clean install build-gen gen
	@echo "✅ 完整重置完成，可以執行 'make dev' 啟動"

# 清理生成的檔案
clean:
	@echo "🧹 清理生成的檔案..."
	rm -rf generator/dist
	rm -rf generator/node_modules
	rm -rf app/src/specstar/generated
	rm -rf app/node_modules
	rm -rf app/dist
	@echo "✅ 清理完成"

# 執行測試
test:
	@echo "🧪 執行測試..."
	cd generator && pnpm test || echo "Generator 沒有測試"
	cd app && pnpm test || echo "App 沒有測試"

# Build production
build: build-gen
	@echo "🏗️  編譯 app 生產版本..."
	cd app && pnpm build
	@echo "✅ 生產版本編譯完成"

# app → generator/templates/base 的同步範圍（排除自動生成的檔案）。
# sync-templates（寫入）與 check-templates（唯讀驗證）共用同一份，兩邊各抄
# 一份的話遲早會漂移，而漂移的後果是「檢查通過但發出去的 template 是舊的」。
TEMPLATE_EXCLUDES := \
	--exclude='node_modules' \
	--exclude='.vite' \
	--exclude='.tanstack' \
	--exclude='dist' \
	--exclude='.env' \
	--exclude='.mantine-version' \
	--exclude='coverage' \
	--exclude='pnpm-lock.yaml' \
	--exclude='src/specstar/generated' \
	--exclude='src/routes/specstar-admin' \
	--exclude='src/routeTree.gen.ts'

# 同步 app → generator/templates/base（排除自動生成的檔案）
# 先在 app 端跑 style（prettier + eslint --fix）確保格式統一
sync-templates:
	@echo "🎨 先格式化 app 程式碼..."
	cd app && make style
	@echo "📋 同步 app → generator/templates/base..."
	rsync -av --delete $(TEMPLATE_EXCLUDES) app/ generator/templates/base/
	@echo "✅ Templates 同步完成"

# 驗證 templates 已經跟 app 同步（不寫入）。
#
# 舊的本機發布路徑會先跑 sync-templates 再打包，CI 不會 —— 它只發 tag 指到
# 的那個 commit。少了這道檢查，「忘記 sync」就會變成一個安靜發出去的舊
# template 版本。release 與 CI 都呼叫這個 target，規則只有一份。
#
# 用 checksum（-c）比對：全新 checkout 的 mtime 是隨機的，rsync 預設的
# size+time 快速檢查會把每個檔案都判成不同。
.PHONY: check-templates
check-templates:
	@drift="$$(rsync -rcin --delete $(TEMPLATE_EXCLUDES) app/ generator/templates/base/)"; \
	if [ -n "$$drift" ]; then \
		echo "❌ generator/templates/base 與 app/ 不同步 —— 請執行 make sync-templates 並 commit"; \
		echo "$$drift"; \
		exit 1; \
	fi; \
	echo "✅ Templates 與 app/ 一致"

# ============================================================================
# App 管理指令
# ============================================================================

# 刪除整個 app 目錄
clean-app:
	@echo "🔥 刪除 app 目錄..."
	@if [ -d "app" ]; then \
		rm -rf app; \
		echo "✅ app 目錄已刪除"; \
	else \
		echo "⚠️  app 目錄不存在"; \
	fi

# 使用 generator 生成全新的 app
gen-app: build-gen
	@echo "🚀 使用 generator 生成全新的 app..."
	@if [ -d "app" ]; then \
		echo "❌ Error: app 目錄已存在，請先執行 'make clean-app'"; \
		exit 1; \
	fi
	@echo "📂 從模板創建新的 app..."
	cd generator && node dist/cli.js init app  --mantine 8 --dir .. $(INIT_TEST_FLAG)
	@echo "📦 安裝 app 依賴..."
	cd app && pnpm install
	@echo "🔧 從 SpecStar 後端生成代碼... ($(API_URL))"
	cd app && node ../generator/dist/cli.js generate --url $(API_URL) --base-path ${BASE_PATH} --proxy-path "/proxy/backend"
	@echo "✅ App 生成完成！"
	@echo ""
	@echo "📝 下一步："
	@echo "   cd app"
	@echo "   pnpm dev"

# 完全重置 app：刪除 + 重新生成 + 安裝依賴（含 test 檔案）
reset-app:
	$(MAKE) clean-app
	$(MAKE) gen-app INCLUDE_TESTS=1
	@echo "✅ App 完全重置完成！"

# 從 SpecStar 後端生成前端代碼
regen-app: build-gen
	@echo "🚀 從 SpecStar 後端重新生成前端代碼..."
	@if [ ! -d "app" ]; then \
		echo "❌ Error: app 目錄不存在，請先執行 'make gen-app'"; \
		exit 1; \
	fi
	@echo "🔧 清除舊的生成代碼..."
	rm -rf app/src/specstar/generated app/src/routes/specstar-admin
	@echo "📡 連接到 $(API_URL)..."
	cd app && node ../generator/dist/cli.js generate --url $(API_URL)
	@echo "✅ 代碼生成完成"

regen-and-run: regen-app dev-app
	@echo "✅ 重新生成並啟動完成！"

# 自動修復 lint + 格式化（commit 前執行一次即可）
style:
	@echo "🎨 Generator: lint --fix + prettier..."
	cd generator && pnpm lint:fix
	cd generator && pnpm format
	@echo "🎨 App: prettier + eslint --fix..."
	@if [ -d "app" ]; then cd app && make style; else echo "⚠️  app 目錄不存在，跳過"; fi
	@echo "✅ Style 完成"

# ============================================================================
# 釋出
# ============================================================================

# 釋出 generator：打 web-vX.Y.Z tag → push。**上傳由 CI 執行**
# （.github/workflows/release-npm.yml）。
#
# 版號從 generator/package.json 讀，所以 tag 一定跟 commit 的內容一致；CI
# 會用 scripts/release_tag.py 再驗一次。tag 前綴 `web-` 讓它跟 Python 套件
# 的 `v*` 分流 —— 兩者版號各走各的，PEP 440 與 SemVer 本來就不互通。
.PHONY: release
release: check-templates
	@git diff --quiet && git diff --cached --quiet || { \
		echo "❌ 工作區不乾淨，請先 commit 再釋出"; exit 1; }; \
	v="$$(node -p "require('./generator/package.json').version")"; \
	[ -n "$$v" ] || { echo "❌ 讀不到 generator/package.json 的 version"; exit 1; }; \
	git rev-parse -q --verify "refs/tags/web-v$$v" >/dev/null && { \
		echo "❌ tag web-v$$v 已存在 —— 該版可能已發佈，npm 不接受重傳"; exit 1; }; \
	echo "release → web-v$$v（上傳由 CI 執行）"; \
	git tag "web-v$$v"; \
	git push origin "web-v$$v"; \
	echo "✅ 已 push tag web-v$$v"; \
	echo "   追蹤：https://github.com/HYChou0515/specstar/actions/workflows/release-npm.yml"

# 一次性：把這個 repo 註冊成 npm 的 trusted publisher，讓 CI 免 token 發布。
#
# 用 CLI 而不是網頁設定頁；三元組必須跟 release-npm.yml 逐字相同，所以寫死
# 在這裡而不是靠人手打。npm 規定 trust 操作要過**帳號層級 2FA**，且明文不
# 接受 bypass-2FA 的 granular token，所以它會跳出 one-time password 提示，
# 只能由維護者本人完成 —— 這是憑證邊界，不是可以自動化的步驟。
#
# 本機 npm 若低於 11 沒有 `trust` 子指令，故走 npx，不動全域安裝。
# 先看會送出什麼：make trust DRY_RUN=1
.PHONY: trust
trust:
	@echo "🔐 註冊 npm trusted publisher（會要求輸入 2FA 六位數）..."
	npx -y npm@11 trust github specstar-web-generator \
		--file release-npm.yml \
		--repo HYChou0515/specstar \
		--env npm \
		--allow-publish $(if $(DRY_RUN),--dry-run)
	@echo "✅ 完成後，發 web-vX.Y.Z tag 即可全自動發布（make release）"

# 一次性的 bootstrap：把 generator 的「第一版」發到 npm。
#
# npm 不能用 OIDC 發套件的第一版 —— trusted publisher 是在套件設定頁設定
# 的，而設定頁要等套件存在才有（npm/cli#8544）。0.3.4 已於 2026-07-28 用這
# 個 target 發過，**不該再用**；往後一律 `make trust`（一次）+ `make
# release`（每次）走 CI。需要先 `npm login`。
.PHONY: bootstrap-publish
bootstrap-publish: style sync-templates build-gen
	@echo "🚀 首次發布 generator 到 npm（bootstrap，只做一次）..."
	cd generator && pnpm check
	cd generator && pnpm test
	cd generator && npm publish
	@echo "✅ 已發布。接著到 npmjs.com 開 trusted publisher，往後用 make release"
