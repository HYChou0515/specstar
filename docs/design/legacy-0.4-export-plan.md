# autocrud 0.4.x → specstar 資料遷移(0.4.6 export → specstar import)— 實作 Plan

> **狀態**:**DONE**(`release/0.4.6` 已 commit + tag `v0.4.6`,本地未 push;specstar 側 PR #449)。後續追加兩項(owner 提問後定案):**0.4.6 `dump(query=)` 增量匯出**、**specstar `load()` 分批 flush**——見文末「追加」。
> **Branch**:`release/0.4.6`(base `v0.4.5`,只 tag 不合回 master)+ `fix/448-legacy-import`(specstar master 側:fixture / 測試 / 文件)
> **Issue**:[#448](https://github.com/HYChou0515/specstar/issues/448)「autocrud v0.4.3 migrate 方法 — 有什麼方法可以安全的 migrate 到新版本? 資料不能丟」
> **動機**:`MIGRATION.md` 只講 0.10 的 `autocrud → specstar` 改名,對 0.4.x 的**儲存格式**完全沒著墨;0.4.x 的磁碟 layout 與 dump 格式和現在完全不同,新版讀不到。定案做法:**出 `autocrud 0.4.6`,讓舊版直接寫出 specstar 現行的 `.acbak` v2 格式**,使用者升版後用現成的 `spec.load()` 匯入。

---

## 調查結論(事實)

| # | 事實 | 影響 |
|---|---|---|
| 1 | 0.4.x 的 `AutoCRUD.dump()` **一呼叫就炸**:`TypeError: Missing required argument 'result'`(`execute_with_events(..., lambda _: {})` 沒給 `OnSuccessDump.result`)。0.4.3 / 0.4.4 / 0.4.5 皆同;實際裝 `autocrud==0.4.3` 驗證過。 | 使用者**無法用舊版匯出**,必須出一個修好的 0.4.x patch。 |
| 2 | 0.4.x 文件唯一推薦的持久化是 `DiskStorageFactory`,layout:`<root>/<model>/meta/<rid>.data` + `<root>/<model>/data/<rid>/<rev>.{data,info}`,全 JSON。0.5.0 起改成 `store/<uid>` + `resource/<rid>/<rev>/<ver>/uid` 兩層。 | 新版 `DiskResourceStore` 讀不到舊 layout;不打算在新版加舊 layout reader。 |
| 3 | `ResourceMeta` / `RevisionInfo` struct 相容:舊 `parent_revision_id` / `schema_version` 是 `str \| UnsetType = UNSET`,新版是 `str \| None = None`;msgpack 省略 UNSET 欄位 → 新版 decode 得到 `None`。新版多的 `rev_*`、`parent_schema_version` 都是 UNSET 預設。 | 舊 struct 直接 msgpack encode 就能被新版 decode,**不需要欄位轉換**。 |
| 4 | 現行 `.acbak` v2 格式(`specstar/resource_manager/dump_format.py`):`[uint32 BE length][msgpack]` framing;tagged struct(`tag_field="t"`)`HeaderRecord(version=2)` → `ModelStartRecord` → `MetaRecord*` / `RevisionRecord*` / `BlobRecord*` → `ModelEndRecord` → `EofRecord`。`RevisionRecord.data` = msgpack(`RawResource(info, raw_data: bytes)`),`raw_data` 是 **manager encoding**(預設 json)的 payload bytes。純 msgspec,無其他依賴。 | 可以原封搬到 0.4.6。 |
| 5 | `data_hash` = `xxh3_128:<hex>` of 「manager encoding 的 payload bytes」;0.4.x 與現行公式相同,encoder 都是 `order="deterministic"`。 | 同 encoding 匯出時 bytes 與磁碟相同、hash 不變;換 encoding 才要重算。 |
| 6 | `v0.4.3 → v0.4.5` 對套件只有純新增:`AutoCRUD(dependency_provider=, permission_checker=, event_handlers=)`、`add_model(event_handlers=, permission_checker=)`、`get_resource_manager()`、`S3ResourceStore(client_kwargs=)`;**行為差異只有「同 model name 註冊兩次改成 raise」**。 | 0.4.6 base 在 `v0.4.5` 對 0.4.3 使用者安全。 |
| 7 | PyPI 上 0.4.0–0.4.5 都已發佈,最新是 0.10.0(shim)。 | patch 版號只能是 **0.4.6**;PyPI 不要求版本時間單調,之後再發 0.4.6 沒問題。 |
| 8 | 0.4.x 沒有 Binary / blob store。 | 匯出不會有 `BlobRecord`。 |

---

## Definition of Done

**`release/0.4.6`**
- [x] `autocrud/resource_manager/dump_format.py`:從 specstar 原封搬入(7 個 record struct + `DumpStreamWriter` / `DumpStreamReader`),tag 名逐字一致
- [x] `RawResource` / `_ExportResourceMeta`(多 `rev_*` 五欄)放在 dump_format,**不動 `types.py` 的 runtime struct**
- [x] `ResourceManager.dump(*, encoding=Encoding.json)` 修好並 yield `MetaRecord` / `RevisionRecord`;`rev_*` 由 current revision 的 `RevisionInfo` 補上;`data_hash` 一律以目標 encoding 的 bytes 重算
- [x] `AutoCRUD.dump(bio, *, encoding="json")` 寫 v2 framed stream(取代 tar);`AutoCRUD.load(bio)` 讀同一格式(取代 tar `load`)
- [x] `autocrud/__init__.py` → `0.4.6`
- [x] `tests/test_dump_load.py`:Disk round-trip + 逐 frame 驗證(順序、`rev_*` 已填、`data_hash` 與磁碟一致、payload bytes 與 `.data` 檔 byte-identical)
- [x] 0.4.5 既有 testsuite 零 regression(289 passed,排除需要 live service 與 wall-clock benchmark 的項目);tag `v0.4.6`(PyPI 發佈由 owner 執行)

**specstar master(`fix/448-legacy-import`)**
- [x] `tests/fixtures/legacy/0_4_x/`:用 0.4.6 分支對真實 0.4.x 資料跑 `dump()` 產出的 `.acbak` + `manifest.json` + 產生腳本(`gen_0_4_x.py`,註明要在舊版 venv 跑)
- [x] `tests/test_legacy_load.py`:`spec.load()` 後逐筆比對 manifest(筆數、revision 鏈、soft-delete、switch 後的 current、datetime/enum/bytes/UUID/nested 欄位、`rev_*` 已填、`indexed_data`);Memory + Disk + SQLite meta store 各跑一次;`on_duplicate` 三種策略
- [x] **預期不改 specstar 程式碼**;若 fixture 載入失敗才改 loader,並記錄原因 → 實際:19 個測試(memory / disk / sqlite)第一次跑就全綠,specstar 零變更
- [x] `docs/en/guides/upgrade-from-0.4.md`(安全流程,見下)+ `guides/index.md`、`mkdocs.yml` nav、`MIGRATION.md` §8 連結、`CHANGELOG.md [Unreleased]`
- [x] PR `Closes #448`;ruff 綠

---

## 定案設計

### 0.4.6:`ResourceManager.dump()`

```python
def dump(self, *, encoding: Encoding = Encoding.json) -> Generator[MetaRecord | RevisionRecord]:
    ser = MsgspecSerializer(encoding=encoding, resource_type=self.resource_type)
    for meta in self.storage.dump_meta():
        export_meta = _ExportResourceMeta(**meta_fields)          # rev_* 預設 UNSET
        try:
            info = self.storage.get_resource_revision_info(meta.resource_id, meta.current_revision_id)
            export_meta.rev_status / rev_created_by / rev_updated_by / rev_created_time / rev_updated_time = info.*
        except Exception:
            pass                                                   # 讀不到就留 UNSET,新版可事後 backfill_revision_meta()
        yield MetaRecord(data=_meta_encoder.encode(export_meta))
    for resource in self.storage.dump_resource():                 # 走正常讀取路徑(含 lazy migration)
        raw = ser.encode(resource.data)
        info = resource.info
        info.data_hash = f"xxh3_128:{xxh3_128_hexdigest(raw)}"     # 同 encoding → 與磁碟相同;換 encoding → 與新版一致
        yield RevisionRecord(data=_raw_encoder.encode(RawResource(info=info, raw_data=raw)))
```

- 修 decorator:`execute_with_events((BeforeDump, AfterDump, OnSuccessDump, OnFailureDump), "result")`;`OnSuccessDump.result` 型別註解改成 `Generator[MetaRecord | RevisionRecord]`(defstruct 不驗證建構值,純文件用途)。
- `dump_resource()` 在 `add_model(migration=...)` 有設時,對舊 `schema_version` 的 revision 會觸發 0.4.x 原有的 **lazy migration 並改寫原檔**——這是 0.4.x 一般 `get()` 就有的行為;文件要求先備份目錄。匯出的 payload 是 migration 後的當前版本,`info.schema_version` 也是當前版本,與磁碟上改寫後的一致。

### 0.4.6:`AutoCRUD.dump()` / `load()`

```python
def dump(self, bio: IO[bytes], *, encoding: Encoding | str = Encoding.json) -> None:
    w = DumpStreamWriter(bio); w.write(HeaderRecord())
    for model_name, mgr in self.resource_managers.items():
        w.write(ModelStartRecord(model_name=model_name))
        for rec in mgr.dump(encoding=Encoding(encoding)): w.write(rec)
        w.write(ModelEndRecord(model_name=model_name))
    w.write(EofRecord())

def load(self, bio: IO[bytes]) -> None:
    # HeaderRecord(version==2) 檢查 → ModelStartRecord 找 manager(找不到 raise ValueError 列出已註冊名)
    # MetaRecord → storage.save_meta(decode ResourceMeta)      (多出的 rev_* 欄位 msgspec 會忽略)
    # RevisionRecord → RawResource → storage.save_resource_revision(Resource(info, data=decode(raw_data)))
    # 0.4.x store API 只吃解碼後的 Resource[T];raw_data 先以 encoding 解碼(從 header 之後的 raw 首 byte 判斷 json/msgpack)
```

- `load()` 保留是為了 `dump/load` 在 0.4.6 內仍成對、能自測 round-trip;舊 tar `load()` 直接移除(0.4.x `dump()` 從沒成功寫出過 tar,無相容包袱)。
- `model_name` 沿用 0.4.x 的 kebab 命名,與 specstar 預設相同;使用者若在新版改了 `model_naming` 或 `name=`,文件註明要對齊。

### specstar 側:預期零程式碼變更

`spec.load()` 現行流程:`HeaderRecord.version == 2` → `ModelStartRecord` 對應 `resource_managers` → `load_records_bulk()`(`MetaRecord` 用 `ResourceMeta` msgpack decoder、`RevisionRecord` 用 `RawResource` decoder → `save_revisions_bulk`)。0.4.6 產的 bytes 與 specstar 自己 `dump()` 的 bytes 結構相同,唯一差異是 `RevisionInfo` 沒有 `parent_schema_version`(→ UNSET,現行讀取路徑已處理 legacy rows)。

---

## 使用者流程(寫進 `upgrade-from-0.4.md`)

```bash
# 1. 舊環境(不動 app 程式碼)
cp -r ./data ./data.bak-0.4                     # 先備份;dump 走正常讀取路徑,有 migration= 時會改寫檔案
pip install autocrud==0.4.6
python -c 'from myapp import crud; crud.dump(open("backup.acbak", "wb"))'

# 2. 升版
pip install -U specstar                         # 照 MIGRATION.md 改 import:autocrud→specstar、AutoCRUD→SpecStar、crud→spec
#    storage 指到「新的空目錄 / 空表」,不要指到舊的 ./data

# 3. 匯入 + 驗證
python -c 'from myapp import spec; print(spec.load(open("backup.acbak", "rb")))'   # {model: LoadStats(loaded=, skipped=, total=)}
#    驗證:每個 model 的 count、抽樣 get()、list_revisions()、soft-deleted 仍為 deleted、switch 過的 current 正確
# 4. 切換流量;舊目錄保留到確認無誤
```

不在範圍(文件註明「開 issue」):0.4.x 手動組 SQLite / Postgres / Redis meta store 或 S3 resource store 的使用者——`dump()` 走 `IStorage` 介面,理論上任何後端都能匯出,但只對 `DiskStorageFactory` 做過測試。

---

## Phases

1. **`release/0.4.6`**:`git checkout -b release/0.4.6 v0.4.5` → 搬 `dump_format.py` → 修 `ResourceManager.dump()` → 改 `AutoCRUD.dump/load` → bump → 測試 → 舊 testsuite 全綠 → tag `v0.4.6`。
2. **fixture**:scratch venv `uv pip install <repo>@release/0.4.6`,對 `tests/fixtures/legacy/` 的 0.4.x 資料產生器(Ticket / Note:datetime、enum、bytes、UUID、nested、list、多 revision、soft-delete、switch、`indexed_fields`)跑 `dump()`,連同 `manifest.json` 進 specstar repo。
3. **specstar 測試**:`tests/test_legacy_load.py` 對 fixture 跑 `spec.load()`;若失敗才改 loader。
4. **文件 + PR**:`upgrade-from-0.4.md`、nav、`MIGRATION.md` §8、CHANGELOG;PR `Closes #448`;issue 回覆由 owner 決定。

---

## 已排除的替代方案

- **新版直接讀 0.4.x disk layout**(`SpecStar.load_legacy_disk(rootdir)`):不用裝舊版,但要在新版永久維護一個舊 layout reader;owner 選擇 export/import 路線。
- **新版 `load()` 自動偵測 0.5–0.8.2 的 tar dump**:與 #448 無關(0.4.x 沒有可用的 tar),不做。
- **在舊環境跑的獨立匯出腳本**:0.4.6 本身就是那個腳本,且能 `pip install`,更好。

---

## 追加(plan 定案後 owner 提問衍生)

### 1. 大量資料的停機時間 → 0.4.6 `dump(query=ResourceMetaSearchQuery)`(commit `68b46a1`,tag 重打)
- 篩選單位是 **resource**;命中的 resource 連同**全部** revision 匯出,所以 delta 用 overwrite 疊在 full 上是 idempotent。
- `updated_time` 在 0.4.x 會被 update / patch / switch / delete / restore 更新(確認過),soft-delete 不會漏。
- `limit` / `offset` 強制 unlimited。
- 流程:`t0 = now()` → full dump(舊系統繼續跑)→ 匯入驗證 → 切換時 `dump(query=updated_time_start=t0)` → `spec.load(delta)`。`t0` 取 full dump **開始**前。
- fixture 加 `delta.acbak` + `manifest_after_delta.json`;`test_full_then_delta_import_equals_the_source_after_cutover` 三後端全綠。

### 2. 大量資料的記憶體峰值 → specstar `SpecStar.load(*, batch_size=1000, batch_bytes=64MiB)`(commit `7b32ded`)
- 原本整個 model section 先 buffer 再一次 `load_records_bulk()`,實測峰值 ≈ **2.3×** section bytes;改成每 N 筆 / M bytes flush,100 筆一批實測 **0.25×**。
- `load_records_bulk(..., skipped_ids=)` 讓 `on_duplicate=skip` 的「被 skip 的 resource 其 revision 也要 skip」跨批次成立(`test_skip_holds_across_flush_boundaries` 用 `batch_size=1` 逼出邊界)。
- `/_backup/import`、`/{model}/import` 改餵 `UploadFile.file`(spooled temp file),不再 `read()` 整包;raw body 路徑本質上仍是整包。
- 沒做:0.4.6 的 `GET /_backup/export` HTTP route(搬家是 owner 跑一次腳本,不需要對外開全量匯出口;0.4.x 預設 `AllowAll`)、0.4.6 per-model `dump(models=...)`。
