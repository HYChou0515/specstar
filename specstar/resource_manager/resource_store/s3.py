from __future__ import annotations

from collections.abc import Generator, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import IO

from specstar.resource_manager.basic import (
    Encoding,
    IResourceStore,
    MsgspecSerializer,
)
from specstar.types import RevisionInfo

try:
    from botocore.exceptions import ClientError as _ClientError
except ImportError:  # pragma: no cover
    _ClientError = None  # type: ignore[assignment,misc]  # ty:ignore[invalid-assignment]


class S3ResourceStore(IResourceStore):
    def __init__(
        self,
        encoding: Encoding = Encoding.json,
        access_key_id: str = "minioadmin",
        secret_access_key: str = "minioadmin",
        region_name: str = "us-east-1",
        endpoint_url: str | None = None,  # minio example:  "http://localhost:9000"
        bucket: str = "specstar",
        prefix: str = "",
        client_kwargs: dict | None = None,
    ):
        import boto3

        self.bucket = bucket
        self.prefix = f"{prefix}resources/"
        self._resource_prefix = f"{self.prefix}resource/"
        self._store_prefix = f"{self.prefix}store/"
        if client_kwargs is None:
            client_kwargs = {}
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region_name,
            **client_kwargs,
        )
        self._info_serializer = MsgspecSerializer(
            encoding=encoding,
            resource_type=RevisionInfo,
        )

        # 確保 bucket 存在
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except _ClientError as e:
            # 檢查是否是 NoSuchBucket 錯誤 (支援 AWS 和 MinIO)
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchBucket", "404"):
                self.client.create_bucket(Bucket=self.bucket)
            else:
                # 其他錯誤則重新拋出
                raise

    def _get_raw_data_key(self, uid: str) -> str:
        """構建實際 data 文件的 S3 key"""
        return f"{self.prefix}store/{uid}/data"

    def _get_raw_info_key(self, uid: str) -> str:
        """構建實際 info 文件的 S3 key"""
        return f"{self.prefix}store/{uid}/info"

    def _get_resource_key(
        self, resource_id: str, revision_id: str, schema_version: str | None
    ) -> str:
        """構建資源索引的 S3 key"""
        if schema_version is None:
            p_schema_version = "no_ver"
        else:
            p_schema_version = f"v_{schema_version}"
        return (
            f"{self._resource_prefix}{resource_id}/{revision_id}/{p_schema_version}/uid"
        )

    def list_resources(self) -> Generator[str]:
        """列出所有資源 ID"""
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.bucket,
            Prefix=self._resource_prefix,
            Delimiter="/",
        )

        for page in page_iterator:
            if "CommonPrefixes" in page:
                for obj in page["CommonPrefixes"]:
                    prefix = obj["Prefix"]
                    # 去除前綴，然後移除末尾斜線，得到資源 ID
                    # 例如: "resources/resource/user1/" -> "user1"
                    resource_id = prefix[len(self._resource_prefix) :].rstrip("/")
                    if resource_id:
                        yield resource_id

    def list_revisions(self, resource_id: str) -> Generator[str]:
        """列出指定資源的所有修訂版本"""
        prefix = f"{self._resource_prefix}{resource_id}/"
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.bucket,
            Prefix=prefix,
            Delimiter="/",
        )

        for page in page_iterator:
            if "CommonPrefixes" in page:
                for obj in page["CommonPrefixes"]:
                    prefix_path = obj["Prefix"]
                    # 提取修訂 ID（去除前綴部分）
                    revision_id = prefix_path[len(prefix) :].rstrip("/")
                    if revision_id:  # 確保不是空字串
                        yield revision_id

    def list_schema_versions(
        self, resource_id: str, revision_id: str
    ) -> Generator[str | None]:
        """列出指定資源修訂版本的所有 schema 版本"""
        prefix = f"{self._resource_prefix}{resource_id}/{revision_id}/"
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.bucket,
            Prefix=prefix,
            Delimiter="/",
        )

        for page in page_iterator:
            if "CommonPrefixes" in page:
                for obj in page["CommonPrefixes"]:
                    prefix_path = obj["Prefix"]
                    # 提取 schema 版本（去除前綴部分）
                    schema_version = prefix_path[len(prefix) :].rstrip("/")
                    if schema_version == "no_ver":
                        yield None
                    elif schema_version.startswith("v_"):
                        yield schema_version[2:]
                    # else:  # 忽略不符合命名規則的資料夾
                    #     continue

    def exists(
        self, resource_id: str, revision_id: str, schema_version: str | None
    ) -> bool:
        """檢查指定的資源修訂版本是否存在"""
        resource_key = self._get_resource_key(resource_id, revision_id, schema_version)
        try:
            self.client.head_object(Bucket=self.bucket, Key=resource_key)
            return True
        except _ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                return False
            raise

    @contextmanager
    def get_data_bytes(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None,
    ) -> Generator[IO[bytes]]:
        """以位元組流的形式獲取指定資源修訂版本的資料"""
        # 先獲取 UID
        resource_key = self._get_resource_key(resource_id, revision_id, schema_version)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=resource_key)
            uid = response["Body"].read().decode("utf-8")
        except _ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                raise KeyError(
                    f"Resource not found: {resource_id}/{revision_id}/{schema_version}"
                )
            raise

        # 使用 UID 獲取實際數據
        data_key = self._get_raw_data_key(uid)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=data_key)
            yield response["Body"]  # ty:ignore[invalid-yield]
            # data_bytes = response["Body"].read()
            # yield io.BytesIO(data_bytes)
        except _ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                raise KeyError(f"Resource data not found: {uid}")
            raise

    def get_revision_info(
        self,
        resource_id: str,
        revision_id: str,
        schema_version: str | None,
    ) -> RevisionInfo:
        """獲取指定修訂版本的資訊"""
        # 先獲取 UID
        resource_key = self._get_resource_key(resource_id, revision_id, schema_version)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=resource_key)
            uid = response["Body"].read().decode("utf-8")
        except _ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                raise KeyError(
                    f"Resource not found: {resource_id}/{revision_id}/{schema_version}"
                )
            raise

        # 使用 UID 獲取實際資訊
        info_key = self._get_raw_info_key(uid)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=info_key)
            info_bytes = response["Body"].read()
            return self._info_serializer.decode(info_bytes)
        except _ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                raise KeyError(f"Revision info not found: {uid}")
            raise

    # -- Bulk read (#434) -------------------------------------------------

    @staticmethod
    def _is_missing(e: "Exception") -> bool:
        return e.response["Error"]["Code"] in ("NoSuchKey", "404")  # ty: ignore[unresolved-attribute]

    def _resolve_uid(self, item: tuple[str, str, str | None]) -> str | None:
        """GET the uid index object; ``None`` when the revision is gone."""
        key = self._get_resource_key(*item)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            return response["Body"].read().decode("utf-8")
        except _ClientError as e:
            if self._is_missing(e):
                return None
            raise

    def _head_size(self, uid: str | None) -> int:
        """``ContentLength`` without transferring the body."""
        if uid is None:
            return 0
        try:
            response = self.client.head_object(
                Bucket=self.bucket, Key=self._get_raw_data_key(uid)
            )
            return response["ContentLength"]
        except _ClientError as e:
            if self._is_missing(e):
                return 0
            raise

    def _get_payload(self, uid: str) -> bytes | None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket, Key=self._get_raw_data_key(uid)
            )
            return response["Body"].read()
        except _ClientError as e:
            if self._is_missing(e):
                return None
            raise

    def read_many(
        self,
        items: "Sequence[tuple[str, str, str | None]]",
        *,
        max_bytes: int,
        max_workers: int = 20,
    ) -> "tuple[dict[str, bytes], int]":
        """Resolve uids, size with ``head_object``, then fetch only what fits.

        This overrides ``read_many`` wholesale instead of the
        ``payload_sizes`` / ``read_payloads`` hooks because reaching a payload
        on S3 takes two calls — GET the uid index, then GET the object — and
        the two hooks would resolve the same uids twice.

        Every stage fans out over a thread pool. That is where the win is: S3
        cost is dominated by per-call latency, so N sequential round-trips is
        precisely the thing worth avoiding. ``head_object`` reports the size
        without sending the body, so the budget is packed exactly rather than
        overshooting by a row.
        """
        items = list(items)
        if not items:
            return {}, 0

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            uids = list(pool.map(self._resolve_uid, items))
            sizes = list(pool.map(self._head_size, uids))

            total = 0
            consumed = 0
            for size in sizes:
                if consumed and total + size > max_bytes:
                    break
                total += size
                consumed += 1

            selected = [
                (item[0], uid)
                for item, uid in zip(items[:consumed], uids[:consumed])
                if uid is not None
            ]
            payloads = list(pool.map(lambda pair: self._get_payload(pair[1]), selected))

        return {
            resource_id: raw
            for (resource_id, _), raw in zip(selected, payloads)
            if raw is not None
        }, consumed

    def save(self, info: RevisionInfo, data: IO[bytes]) -> None:
        # 保存實際數據和資訊到 UID-based 位置
        self._save_raw_data(str(info.uid), data)
        self._save_raw_info(info)
        # 建立資源索引，指向 UID
        self._create_resource_index(
            info.resource_id, info.revision_id, info.schema_version, str(info.uid)
        )

    def save_many(
        self,
        items: "Iterable[tuple[RevisionInfo, bytes | IO[bytes]]]",
        max_workers: int = 20,
    ) -> None:
        """Bulk save multiple revisions with concurrent S3 PUTs.

        Each *item* is ``(info, data)`` where *data* is raw bytes **or**
        an ``IO[bytes]`` file-like object.

        All underlying ``put_object`` calls (3 per revision: data, info,
        index) are submitted concurrently via a :class:`ThreadPoolExecutor`.
        """

        item_list = list(items)
        if not item_list:
            return

        bucket = self.bucket
        client = self.client
        info_encode = self._info_serializer.encode

        def _put_one(info: RevisionInfo, raw: bytes) -> None:
            uid = str(info.uid)
            # 1. data
            client.put_object(
                Bucket=bucket,
                Key=self._get_raw_data_key(uid),
                Body=raw,
            )
            # 2. info
            client.put_object(
                Bucket=bucket,
                Key=self._get_raw_info_key(uid),
                Body=info_encode(info),
            )
            # 3. resource index
            resource_key = self._get_resource_key(
                info.resource_id, info.revision_id, info.schema_version
            )
            client.put_object(
                Bucket=bucket,
                Key=resource_key,
                Body=uid.encode("utf-8"),
            )

        # Materialise raw bytes once, outside the pool
        prepared: list[tuple[RevisionInfo, bytes]] = []
        for info, data in item_list:
            raw = data.read() if hasattr(data, "read") else data  # ty:ignore[call-non-callable]
            prepared.append((info, raw))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futs = [pool.submit(_put_one, info, raw) for info, raw in prepared]
            for f in futs:
                f.result()  # raise on first error

    def _save_raw_data(self, uid: str, data: IO[bytes]) -> None:
        """保存資源修訂版本的資料到 UID-based 位置"""
        data_key = self._get_raw_data_key(uid)
        self.client.put_object(Bucket=self.bucket, Key=data_key, Body=data.read())

    def _save_raw_info(self, info: RevisionInfo) -> None:
        """保存資源修訂版本的資訊到 UID-based 位置"""
        info_key = self._get_raw_info_key(str(info.uid))
        info_bytes = self._info_serializer.encode(info)
        self.client.put_object(Bucket=self.bucket, Key=info_key, Body=info_bytes)

    def _create_resource_index(
        self, resource_id: str, revision_id: str, schema_version: str | None, uid: str
    ) -> None:
        """建立資源索引，指向實際的 UID"""
        resource_key = self._get_resource_key(resource_id, revision_id, schema_version)
        self.client.put_object(
            Bucket=self.bucket, Key=resource_key, Body=uid.encode("utf-8")
        )

    def cleanup(self) -> None:
        """清理所有以指定前綴開頭的 S3 物件"""
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)

        objects_to_delete = []
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    objects_to_delete.append({"Key": obj["Key"]})

        # 批量刪除物件
        if objects_to_delete:
            # S3 批量刪除每次最多1000個物件
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i : i + 1000]
                self.client.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": batch},
                )

    # ------------------------------------------------------------------
    # Bulk dump helpers
    # ------------------------------------------------------------------

    def dump_all_revisions(
        self,
        *,
        resource_ids: frozenset[str] | None = None,
        max_workers: int = 10,
    ) -> dict[str, list[tuple[RevisionInfo, bytes]]] | None:
        """Bulk-export all revisions with a single listing + concurrent fetches.

        Instead of 6 serial S3 calls per resource (2 listings + 4 GETs),
        this method:

        1. Does **one** paginated ``ListObjectsV2`` on the *resource*
           prefix to discover uid-index keys (which embed the
           ``resource_id`` in their path, enabling early filtering).
        2. Uses a :class:`ThreadPoolExecutor` to concurrently resolve
           the uid values **and** fetch the corresponding info + data
           objects.

        Returns a dict mapping ``resource_id → [(RevisionInfo, raw_bytes)]``.
        When *resource_ids* is given only those resources are fetched.
        """
        # Step 1: single listing of all uid-index keys under resource/
        # Key format: {resource_prefix}{rid}/{rev}/{sv}/uid
        uid_keys: list[str] = []
        prefix_len = len(self._resource_prefix)
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self.bucket,
            Prefix=self._resource_prefix,
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("/uid"):
                    continue
                # Early filter by resource_id (embedded in path)
                if resource_ids is not None:
                    parts = key[prefix_len:].split("/", 1)
                    if parts[0] not in resource_ids:
                        continue
                uid_keys.append(key)

        if not uid_keys:
            return {}

        # Step 2: concurrent resolve uid + fetch info + data
        bucket = self.bucket
        client = self.client
        info_decode = self._info_serializer.decode
        get_raw_info_key = self._get_raw_info_key
        get_raw_data_key = self._get_raw_data_key

        def _fetch_one(uid_key: str) -> tuple[RevisionInfo, bytes]:
            # Resolve uid value from index object
            uid = (
                client.get_object(Bucket=bucket, Key=uid_key)["Body"]
                .read()
                .decode("utf-8")
            )
            # Fetch info + data using resolved uid
            info_resp = client.get_object(Bucket=bucket, Key=get_raw_info_key(uid))
            data_resp = client.get_object(Bucket=bucket, Key=get_raw_data_key(uid))
            info = info_decode(info_resp["Body"].read())
            data = data_resp["Body"].read()
            return info, data

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            fetched = list(pool.map(_fetch_one, uid_keys))

        # Step 3: group by resource_id
        result: dict[str, list[tuple[RevisionInfo, bytes]]] = {}
        for info, data in fetched:
            result.setdefault(info.resource_id, []).append((info, data))
        return result
