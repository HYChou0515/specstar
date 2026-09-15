# SpecStar 開發與文檔 Makefile

# 變數設定
MKDOCS       ?= uv run mkdocs
DOCSDIR      = docs
SITEDIR      = site

# 默認目標
.PHONY: help
help:
	@echo "SpecStar 開發與文檔工具"
	@echo ""
	@echo "開發工具："
	@echo "  test         執行所有測試（排除基準測試）"
	@echo "  test-benchmark 執行基準測試（需要外部系統依賴）"
	@echo "  benchmark    執行基準測試腳本並更新文檔（MetaStore/ResourceStore）"
	@echo "  bench        執行 CRUD benchmark（config-driven）"
	@echo "  bench-list   列出 CRUD benchmark 所有 scenario"
	@echo "  coverage     執行測試並生成覆蓋率報告"
	@echo "  cov-html     生成 HTML 覆蓋率報告"
	@echo "  style        格式化程式碼並修復程式碼風格問題 (ruff format + ruff check --fix)"
	@echo "  check        檢查程式碼品質 (ruff check)"
	@echo "  format       格式化程式碼 (ruff format)"
	@echo "  lint         執行 lint 檢查"
	@echo "  stubs        重新產生 specstar/events.pyi 型別 stub"
	@echo "  stubs-check  檢查 events.pyi 是否與生成器同步（CI 用）"
	@echo "  install      安裝專案依賴"
	@echo "  dev-install  安裝開發依賴"
	@echo "  build        建置套件"
	@echo "  clean        清理所有暫存和構建文件 (clean-dev + clean-docs)"
	@echo "  clean-dev    清理開發暫存檔案"
	@echo ""
	@echo "釋出："
	@echo "  changelog-preview  預覽尚未釋出的變更紀錄"
	@echo "  release <step>     bump 版本 + 生成 CHANGELOG + commit"
	@echo "                     step = alpha|beta|rc|final|patch|minor|major"
	@echo "                     或 make release VERSION=X.Y.Z"
	@echo "  release-publish    打 tag + push；上傳由 CI 執行（不可逆）"
	@echo "                     npm generator 的釋出見 make -C web help"
	@echo ""
	@echo "文檔工具："
	@echo "  docs         構建 MkDocs HTML 文檔"
	@echo "  serve        啟動本地文檔服務器（http://127.0.0.1:8000）"
	@echo "  deploy-docs  部署文檔到 GitHub Pages"
	@echo "  clean-docs   清理文檔構建文件"
	@echo "  docs-check   檢查文檔配置"
	@echo "  docs-quick   快速構建文檔（不清理）"
	@echo "  all-docs     清理並構建完整文檔"
	@echo ""
	@echo "複合指令："
	@echo "  quality      完整的程式碽品質檢查 (style + check + test)"
	@echo "  dev          快速開發循環 (style + test)"
	@echo "  ci           CI/CD 流程 (check + test + coverage)"
	@echo "  full-check   完整檢查 (dev-install + quality + coverage)"

# === 開發工具 ===

# 安裝依賴
.PHONY: install
install:
	@echo "安裝專案依賴..."
	uv sync

# 安裝開發依賴
.PHONY: dev-install
dev-install:
	@echo "安裝開發依賴..."
	uv sync --dev

# 執行測試（排除基準測試）
# Mirrors the CI fast job: documentation examples + main pytest suite.
# pytest-markdown-docs catches README / docs code-block drift; tag
# illustrative snippets with ```python notest``` to skip them.
.PHONY: test
test: check
	@echo "執行文檔範例測試（README markdown blocks）..."
	uv run pytest --markdown-docs README.md -q
	@echo "執行測試（排除基準測試）..."
	uv run coverage run --branch -m pytest -m "not benchmark"
	uv run coverage report -m

# 執行基準測試
.PHONY: test-benchmark
test-benchmark:
	@echo "執行基準測試（需要外部系統依賴）..."
	uv run pytest -m "benchmark" -v

# 執行基準測試腳本並更新文檔（MetaStore/ResourceStore 效能圖表）
.PHONY: benchmark
benchmark:
	@echo "執行基準測試腳本並更新文檔..."
	uv run --with matplotlib --with seaborn scripts/run_benchmarks.py

# 執行 CRUD benchmark（config-driven）
.PHONY: bench
bench:
	@echo "執行 CRUD benchmark..."
	uv run --extra benchmark python -m benchmark

# 列出 CRUD benchmark 所有 scenario
.PHONY: bench-list
bench-list:
	@echo "列出 CRUD benchmark scenario..."
	uv run --extra benchmark python -m benchmark --list

# 執行測試並生成覆蓋率報告
.PHONY: coverage
coverage: test
	@echo "生成覆蓋率報告..."

# 生成 HTML 覆蓋率報告
.PHONY: cov-html
cov-html: coverage
	@echo "生成 HTML 覆蓋率報告..."
	uv run coverage html
	@echo "HTML 報告已生成在 htmlcov/ 目錄"

# 程式碼格式化和修復
.PHONY: style
style:
	@echo "格式化程式碼並修復程式碼風格問題..."
	uv run ruff format .
	uv run ruff check --fix .

# 檢查程式碼品質
.PHONY: check
check:
	@echo "檢查程式碼品質..."
	uv run ruff check .
	uv run ruff format --check

# 格式化程式碼
.PHONY: format
format:
	@echo "格式化程式碼..."
	uv run ruff format .

# Lint 檢查
.PHONY: lint
lint: check
	@echo "Lint 檢查完成"

# 重新生成 .pyi stub（目前只生 specstar/events.pyi）
.PHONY: stubs
stubs:
	@echo "產生 events.pyi stub..."
	uv run python scripts/gen_events_stub.py > specstar/events.pyi
	uv run ruff format specstar/events.pyi

# 確認 stub 與生成器同步（CI 用）
.PHONY: stubs-check
stubs-check:
	@echo "檢查 events.pyi 是否與生成器同步..."
	@cp specstar/events.pyi /tmp/events.pyi.expected
	@$(MAKE) stubs >/dev/null
	@diff -u /tmp/events.pyi.expected specstar/events.pyi || ( \
		cp /tmp/events.pyi.expected specstar/events.pyi; \
		echo "events.pyi 過期 — 請執行 make stubs"; \
		exit 1)
	@rm -f /tmp/events.pyi.expected

# 預覽尚未釋出的變更紀錄（從 conventional commits 生成；非規範 commit 會被忽略）
.PHONY: changelog-preview
changelog-preview:
	uv run git-cliff --unreleased

# 釋出第一步:bump 版本 → 生成 CHANGELOG 區段 → commit "bump vX.Y.Z"。
# 用法:  make release alpha        # 0.13.0a1 → 0.13.0a2(0.12.3 → 0.13.0a1)
#        make release beta         # 0.13.0a2 → 0.13.0b1
#        make release rc           # 0.13.0b1 → 0.13.0rc1
#        make release final        # 0.13.0rc1 → 0.13.0(結束預覽期)
#        make release patch        # 0.11.1 → 0.11.2
#        make release minor        # 0.11.1 → 0.12.0
#        make release major        # 0.11.1 → 1.0.0
#        make release VERSION=1.2.3 # 指定明確版本
#
# 版本由 scripts/next_version.py 依 PEP 440 算出,**不是** git-cliff。
# git-cliff 把 tag 當 SemVer 解析,而我們發的是 PEP 440(0.13.0a1,而非
# 0.13.0-alpha.1),所以 v0.13.0a1 一被打上去,--bumped-version 就整個報
# Semver error —— patch/minor/major 也一起靜默壞掉。版號算術因此留在
# repo 內(有測試:tests/test_next_version.py),git-cliff 只負責它擅長的
# 事:把 commit 變成 CHANGELOG 文字。
#
# 之後接原本流程(打 tag → build → PyPI)。
.PHONY: release alpha beta rc final patch minor major
# 這些是 release 的參數,被當成 no-op goal 消化掉
alpha beta rc final patch minor major:
	@:
release:
	@git diff --quiet && git diff --cached --quiet || { \
		echo "工作區不乾淨,請先 commit 或 stash 再 release"; exit 1; }; \
	step="$(filter alpha beta rc final patch minor major,$(MAKECMDGOALS))"; \
	if [ -n "$(VERSION)" ]; then new="$(VERSION)"; \
	elif [ -n "$$step" ]; then \
		cur="$$(sed -n 's/^__version__ = "\(.*\)"/\1/p' specstar/__init__.py)"; \
		new="$$(uv run python scripts/next_version.py "$$cur" "$$step")" || exit 1; \
	else echo "用法: make release alpha|beta|rc|final|patch|minor|major  或  make release VERSION=X.Y.Z"; exit 1; fi; \
	[ -n "$$new" ] || { echo "無法決定版本"; exit 1; }; \
	git rev-parse -q --verify "refs/tags/v$$new" >/dev/null && { \
		echo "tag v$$new 已存在 —— 該版可能已發佈,PyPI 不接受重傳"; exit 1; }; \
	echo "release → v$$new"; \
	sed -i "s/^__version__ = .*/__version__ = \"$$new\"/" specstar/__init__.py; \
	uv run git-cliff --unreleased --tag "v$$new" --prepend CHANGELOG.md || { \
		echo "git-cliff 失敗 —— 已回復 __version__。不留下「版號有動、CHANGELOG 沒動」的半套 bump"; \
		git checkout -- specstar/__init__.py; exit 1; }; \
	git add specstar/__init__.py CHANGELOG.md; \
	git commit -m "bump v$$new"; \
	echo "✅ 已 commit \"bump v$$new\"。接著:make release-publish"

# 釋出第二步:打 tag → push。**上傳由 CI 執行**。
# 從 specstar/__init__.py 讀版號,所以一定跟上一步 commit 的內容一致。
# tag 是 lightweight(與既有 v0.12.x 慣例一致),必須明確 push。
#
# push 之後 .github/workflows/release-pypi.yml 會接手:重新驗證 tag 與
# __version__ 一致 → build → 用 OIDC trusted publishing 上傳 PyPI。本機
# 不再持有 PyPI token,也不再有第二條上傳路徑 —— 兩條並存的話,tag 守衛
# 就只是「大部分時候會擋」而已。
#
# build + twine check 仍留在本機當 pre-flight:tag 一旦 push 出去就會觸發
# 不可逆的上傳,metadata 壞掉要在推之前就知道,而不是留下一個懸空的 tag。
.PHONY: release-publish
release-publish:
	@git diff --quiet && git diff --cached --quiet || { \
		echo "工作區不乾淨,請先 commit 再發佈"; exit 1; }; \
	v="$$(sed -n 's/^__version__ = "\(.*\)"/\1/p' specstar/__init__.py)"; \
	[ -n "$$v" ] || { echo "讀不到 __version__"; exit 1; }; \
	echo "release → v$$v(上傳由 CI 執行)"; \
	rm -rf dist; \
	uv build || exit 1; \
	uvx twine check dist/* || exit 1; \
	git rev-parse -q --verify "refs/tags/v$$v" >/dev/null || git tag "v$$v"; \
	git push origin "v$$v"; \
	echo "✅ 已 push tag v$$v"; \
	echo "   追蹤:https://github.com/HYChou0515/specstar/actions/workflows/release-pypi.yml"

# 清理所有暫存和構建文件
.PHONY: clean
clean: clean-dev clean-docs
	@echo "所有暫存和構建文件已清理"

# 清理開發暫存檔案
.PHONY: clean-dev
clean-dev:
	@echo "清理開發暫存檔案..."
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info/
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete

# 建置 specstar 套件
.PHONY: build
build: clean-dev
	@echo "建置 specstar..."
	uv build

# 建置 autocrud-shim（autocrud 0.10.0 deprecation 套件）
.PHONY: build-shim
build-shim:
	@echo "建置 autocrud-shim..."
	cd autocrud-shim && uv build

# 同時建置 specstar 與 autocrud-shim 兩個 wheel
.PHONY: build-all
build-all: build build-shim
	@echo "兩個 wheel 都建置完成："
	@ls -1 dist/*.whl autocrud-shim/dist/*.whl

# 上傳 PyPI 的唯一路徑是 `make release-publish` 推 tag → CI(見
# .github/workflows/release-pypi.yml)。這裡曾經有一個 `publish` target 跑
# scripts/publish.py 直接 twine upload;它繞過 tag 與 __version__ 的一致性
# 檢查,留著就等於那個檢查隨時可以被繞過。已隨腳本一起移除。

# === 複合指令 ===

# 完整的程式碼品質檢查
.PHONY: quality
quality: style check test
	@echo "程式碼品質檢查完成"

# 快速開發循環
.PHONY: dev
dev: style test
	@echo "開發循環完成"

# CI/CD 流程
.PHONY: ci
ci: check test coverage
	@echo "CI 流程完成"

# 完整檢查（發布前）
.PHONY: full-check
full-check: clean-dev dev-install quality coverage
	@echo "完整檢查完成"

# === 文檔工具 ===

# 構建 HTML 文檔
.PHONY: docs
docs:
	@echo "構建 MkDocs 文檔..."
	$(MKDOCS) build --clean
	@echo ""
	@echo "HTML 文檔構建完成。文檔位置："
	@echo "  file://$(PWD)/$(SITEDIR)/index.html"

# 啟動本地文檔服務器
.PHONY: serve
serve:
	@echo "啟動 MkDocs 開發服務器於 http://127.0.0.1:8000"
	$(MKDOCS) serve

# 部署文檔到 GitHub Pages
.PHONY: deploy-docs
deploy-docs:
	@echo "部署文檔到 GitHub Pages..."
	$(MKDOCS) gh-deploy --force

# 清理文檔構建文件
.PHONY: clean-docs
clean-docs:
	@echo "清理文檔構建文件..."
	rm -rf "$(SITEDIR)"
	@echo "文檔構建文件已清理"

# 檢查文檔連結
.PHONY: docs-check
docs-check:
	@echo "檢查文檔配置..."
	$(MKDOCS) build --strict

# 快速構建文檔（不清理）
.PHONY: docs-quick
docs-quick:
	@echo "快速構建文檔..."
	$(MKDOCS) build

# 構建所有文檔格式
.PHONY: all-docs
all-docs: clean-docs docs
	@echo "所有文檔格式構建完成"

# 向後相容的別名
.PHONY: html
html: docs

.PHONY: livehtml
livehtml: serve

