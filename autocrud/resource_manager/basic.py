import functools
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable, MutableMapping
from contextlib import contextmanager
from contextvars import ContextVar
from enum import Flag, StrEnum
from typing import Any, Generic, TypeVar

import msgspec
from msgspec import UNSET, Struct, UnsetType

from autocrud.types import ResourceMeta
from autocrud.types import ResourceMetaSearchQuery
from autocrud.types import DataSearchCondition
from autocrud.types import DataSearchOperator
from autocrud.types import ResourceDataSearchSort
from autocrud.types import ResourceMetaSearchSort
from autocrud.types import ResourceMetaSortDirection
from autocrud.types import ResourceMetaSortKey
from autocrud.types import RevisionInfo
from autocrud.types import Resource

T = TypeVar("T")


class Ctx(Generic[T]):
    def __init__(self, name: str, *, strict_type: type[T] | UnsetType = UNSET):
        self.strict_type = strict_type
        self.v = ContextVar[T](name)
        self.tok = None

    @contextmanager
    def ctx(self, value: T):
        if self.strict_type is not UNSET and not isinstance(value, self.strict_type):
            raise TypeError(f"Context value must be of type {self.strict_type}")
        self.tok = self.v.set(value)
        try:
            yield
        finally:
            if self.tok is not None:
                self.v.reset(self.tok)
                self.tok = None

    def get(self) -> T:
        return self.v.get()


class Encoding(StrEnum):
    json = "json"
    msgpack = "msgpack"


def is_match_query(meta: ResourceMeta, query: ResourceMetaSearchQuery) -> bool:
    if query.is_deleted is not UNSET and meta.is_deleted != query.is_deleted:
        return False

    if (
        query.created_time_start is not UNSET
        and meta.created_time < query.created_time_start
    ):
        return False
    if (
        query.created_time_end is not UNSET
        and meta.created_time > query.created_time_end
    ):
        return False
    if (
        query.updated_time_start is not UNSET
        and meta.updated_time < query.updated_time_start
    ):
        return False
    if (
        query.updated_time_end is not UNSET
        and meta.updated_time > query.updated_time_end
    ):
        return False

    if query.created_bys is not UNSET and meta.created_by not in query.created_bys:
        return False
    if query.updated_bys is not UNSET and meta.updated_by not in query.updated_bys:
        return False

    if query.data_conditions is not UNSET and meta.indexed_data is not UNSET:
        for condition in query.data_conditions:
            if not _match_data_condition(meta.indexed_data, condition):
                return False
    elif query.data_conditions is not UNSET:
        # 如果有 data 條件但沒有索引資料，不匹配
        return False

    return True


def _match_data_condition(
    indexed_data: dict[str, Any],
    condition: DataSearchCondition,
) -> bool:
    """檢查索引資料是否匹配 data 條件"""
    field_value = indexed_data.get(condition.field_path)

    if condition.operator == DataSearchOperator.equals:
        return field_value == condition.value
    if condition.operator == DataSearchOperator.not_equals:
        return field_value != condition.value
    if condition.operator == DataSearchOperator.greater_than:
        return field_value is not None and field_value > condition.value
    if condition.operator == DataSearchOperator.greater_than_or_equal:
        return field_value is not None and field_value >= condition.value
    if condition.operator == DataSearchOperator.less_than:
        return field_value is not None and field_value < condition.value
    if condition.operator == DataSearchOperator.less_than_or_equal:
        return field_value is not None and field_value <= condition.value
    if condition.operator == DataSearchOperator.contains:
        # 特殊處理：如果 field_value 是列表，檢查 condition.value 是否在列表中
        if isinstance(field_value, list):
            return condition.value in field_value
        if isinstance(condition.value, Flag) and isinstance(field_value, int):
            return (condition.value.value & field_value) == condition.value.value
        # 標準字符串包含檢查
        return field_value is not None and str(condition.value) in str(field_value)
    if condition.operator == DataSearchOperator.starts_with:
        return field_value is not None and str(field_value).startswith(
            str(condition.value),
        )
    if condition.operator == DataSearchOperator.ends_with:
        return field_value is not None and str(field_value).endswith(
            str(condition.value),
        )
    if condition.operator == DataSearchOperator.in_list:
        return (
            field_value in condition.value
            if isinstance(condition.value, (list, tuple, set))
            else False
        )
    if condition.operator == DataSearchOperator.not_in_list:
        return (
            field_value not in condition.value
            if isinstance(condition.value, (list, tuple, set))
            else True
        )

    return False


def bool_to_sign(b: bool) -> int:
    return 1 if b else -1


def get_sort_fn(qsorts: list[ResourceMetaSearchSort | ResourceDataSearchSort]):
    def compare(meta1: ResourceMeta, meta2: ResourceMeta) -> int:
        for sort in qsorts:
            if isinstance(sort, ResourceMetaSearchSort):
                if sort.key == ResourceMetaSortKey.created_time:
                    if meta1.created_time != meta2.created_time:
                        return bool_to_sign(meta1.created_time > meta2.created_time) * (
                            1
                            if sort.direction == ResourceMetaSortDirection.ascending
                            else -1
                        )
                elif sort.key == ResourceMetaSortKey.updated_time:
                    if meta1.updated_time != meta2.updated_time:
                        return bool_to_sign(meta1.updated_time > meta2.updated_time) * (
                            1
                            if sort.direction == ResourceMetaSortDirection.ascending
                            else -1
                        )
                elif sort.key == ResourceMetaSortKey.resource_id:
                    if meta1.resource_id != meta2.resource_id:
                        return bool_to_sign(meta1.resource_id > meta2.resource_id) * (
                            1
                            if sort.direction == ResourceMetaSortDirection.ascending
                            else -1
                        )
            else:
                v1 = meta1.indexed_data.get(sort.field_path)
                v2 = meta2.indexed_data.get(sort.field_path)
                if v1 != v2:
                    return bool_to_sign(v1 > v2) * (
                        1
                        if sort.direction == ResourceMetaSortDirection.ascending
                        else -1
                    )
        return 0

    return functools.cmp_to_key(compare)


class MsgspecSerializer(Generic[T]):
    def __init__(self, encoding: Encoding, resource_type: type[T]):
        self.encoding = encoding
        if self.encoding == "msgpack":
            self.encoder = msgspec.msgpack.Encoder(order="deterministic")
            self.decoder = msgspec.msgpack.Decoder(resource_type)
        else:
            self.encoder = msgspec.json.Encoder(order="deterministic")
            self.decoder = msgspec.json.Decoder(resource_type)

    def encode(self, obj: T) -> bytes:
        return self.encoder.encode(obj)

    def decode(self, b: bytes) -> T:
        return self.decoder.decode(b)


class IMetaStore(MutableMapping[str, ResourceMeta]):
    """Interface for a metadata store that manages resource metadata.

    This interface provides a dictionary-like interface for storing and retrieving
    resource metadata, with additional search capabilities. It serves as the primary
    storage mechanism for ResourceMeta objects in the AutoCRUD system.

    The store can be used like a standard Python dictionary, with resource IDs as keys
    and ResourceMeta objects as values. It extends the MutableMapping interface with
    search functionality to support complex queries.

    See: https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping
    """

    @abstractmethod
    def __getitem__(self, pk: str) -> ResourceMeta:
        """Get resource metadata by resource ID.

        Arguments:
            pk (str): The resource ID (primary key) to retrieve metadata for.

        Returns:
            ResourceMeta: The metadata object for the specified resource.

        Raises:
            KeyError: If the resource ID does not exist in the store.
        """

    @abstractmethod
    def __setitem__(self, pk: str, b: ResourceMeta) -> None:
        """Store resource metadata by resource ID.

        Arguments:
            pk (str): The resource ID (primary key) to store metadata under.
            b (ResourceMeta): The metadata object to store.

        ---
        This method stores or updates the metadata for a resource. If the resource ID
        already exists, the metadata will be replaced. The implementation should ensure
        the metadata is persisted according to the store's persistence strategy.
        """

    @abstractmethod
    def __delitem__(self, pk: str) -> None:
        """Delete resource metadata by resource ID.

        Arguments:
            pk (str): The resource ID (primary key) to delete metadata for.

        Raises:
            KeyError: If the resource ID does not exist in the store.

        ---
        This method permanently removes the metadata for a resource from the store.
        Note that this is different from soft deletion - this completely removes
        the metadata record from storage.
        """

    @abstractmethod
    def __iter__(self) -> Generator[str]:
        """Iterate over all resource IDs in the store.

        Returns:
            Generator[str]: A generator yielding all resource IDs in the store.

        ---
        This method allows iteration over all resource IDs currently stored in the
        metadata store. The order of iteration may vary depending on the implementation.
        """

    @abstractmethod
    def __len__(self) -> int:
        """Return the total number of resources in the store.

        Returns:
            int: The count of all resource metadata records in the store.

        ---
        This method provides the total count of all resource metadata records,
        including both active and soft-deleted resources.
        """

    @abstractmethod
    def iter_search(self, query: ResourceMetaSearchQuery) -> Generator[ResourceMeta]:
        """Search for resource metadata based on query criteria.

        Arguments:
            query (ResourceMetaSearchQuery): The search criteria including filters,
                sorting, and pagination options.

        Returns:
            Generator[ResourceMeta]: A generator yielding ResourceMeta objects that
                match the query criteria.

        ---
        This method performs a search across all resource metadata using the provided
        query parameters. The query supports:
        - Filtering by deletion status, timestamps, and user information
        - Data content filtering based on indexed_data fields
        - Sorting by metadata fields or data content fields
        - Pagination with limit and offset

        The results are yielded in the order specified by the sort criteria and
        limited by the pagination parameters.
        """


class IFastMetaStore(IMetaStore):
    """Interface for a fast, temporary metadata store with bulk operations.

    This interface extends IMetaStore with additional capabilities for high-performance
    temporary storage. It's designed for scenarios where metadata needs to be quickly
    stored and then bulk-transferred to a slower, more persistent store.

    Fast meta stores are typically implemented using in-memory storage (like Redis or
    memory-based stores) and provide optimized performance for frequent read/write
    operations at the cost of potential data loss if the store goes down.

    The key feature is the get_then_delete operation which enables efficient bulk
    synchronization with slower storage systems while maintaining data consistency.
    """

    @abstractmethod
    @contextmanager
    def get_then_delete(self) -> Generator[Iterable[ResourceMeta]]:
        """Atomically retrieve all metadata and mark it for deletion.

        Returns:
            Generator[Iterable[ResourceMeta]]: A context manager that yields an
                iterable of all ResourceMeta objects currently in the store.

        ---
        This method provides an atomic operation that:
        1. Retrieves all metadata currently stored in the fast store
        2. Marks that metadata for deletion
        3. Actually deletes it only if the context exits successfully

        If an exception occurs within the context, the metadata will NOT be deleted,
        ensuring data consistency during bulk transfer operations.

        Use Cases:
        - Bulk synchronization from fast storage to slow storage
        - Batch processing of accumulated metadata
        - Atomic transfer operations between storage tiers

        Example:
            ```python
            with fast_store.get_then_delete() as metas:
                slow_store.save_many(metas)
                # Metadata is deleted from fast store only if save_many succeeds
            ```

        The deletion occurs only after the context manager exits successfully,
        providing transactional semantics for bulk operations.
        """


class ISlowMetaStore(IMetaStore):
    """Interface for a persistent, durable metadata store with batch operations.

    This interface extends IMetaStore with capabilities optimized for persistent
    storage systems. Slow meta stores prioritize data durability and consistency
    over raw performance, making them suitable for long-term metadata storage.

    These stores are typically implemented using persistent storage systems like
    databases (PostgreSQL, SQLite) or distributed storage systems, providing
    guarantees about data persistence even in the event of system failures.

    The key feature is the save_many operation which enables efficient bulk
    insertion and update operations, optimizing performance for batch scenarios
    while maintaining the durability guarantees of persistent storage.
    """

    @abstractmethod
    def save_many(self, metas: Iterable[ResourceMeta]) -> None:
        """Bulk save operation for multiple resource metadata objects.

        Arguments:
            metas (Iterable[ResourceMeta]): An iterable of ResourceMeta objects
                to be saved to persistent storage.

        ---
        This method provides an optimized bulk save operation that can efficiently
        handle multiple metadata objects in a single transaction or batch operation.
        It's designed to minimize the overhead of individual save operations when
        dealing with large numbers of metadata objects.

        Behavior:
        - All metadata objects are saved atomically where possible
        - Existing metadata with the same resource_id will be updated
        - New metadata objects will be inserted
        - The operation should be optimized for the underlying storage system

        Use Cases:
        - Bulk synchronization from fast storage to persistent storage
        - Initial data loading and migration operations
        - Batch processing scenarios with multiple metadata updates
        - Periodic bulk backup operations

        Performance Considerations:
        - Implementation should use batch operations where available
        - Transaction boundaries should be optimized for the storage system
        - Error handling should provide partial success information where possible

        The method may raise storage-specific exceptions if the bulk operation fails,
        and implementations should provide appropriate error handling and rollback
        mechanisms where supported by the underlying storage system.
        """


class IResourceStore(ABC, Generic[T]):
    """Interface for storing and retrieving versioned resource data.

    This interface manages the storage of actual resource data and their revision
    information. Unlike metadata stores that handle ResourceMeta objects, resource
    stores manage the complete Resource[T] objects including both data content and
    revision information.

    The store provides version control capabilities by maintaining all revisions
    of each resource, allowing for complete history tracking and point-in-time
    recovery. Each resource can have multiple revisions, and each revision
    contains both the data at that point in time and metadata about the revision.

    Type Parameters:
        T: The type of data stored in resources. This allows the store to be
           type-safe for specific resource data structures.
    """

    @abstractmethod
    def list_resources(self) -> Generator[str]:
        """Iterate over all resource IDs in the store.

        Returns:
            Generator[str]: A generator yielding all unique resource IDs that
                have at least one revision stored in the system.

        ---
        This method provides access to all resource identifiers currently stored
        in the system, regardless of their deletion status or number of revisions.
        Each resource ID represents a unique resource that may have one or more
        revisions stored.

        The iteration order is implementation-dependent and may vary between
        different storage backends. For consistent ordering, use appropriate
        sorting mechanisms in the calling code.
        """

    @abstractmethod
    def list_revisions(self, resource_id: str) -> Generator[str]:
        """Iterate over all revision IDs for a specific resource.

        Arguments:
            resource_id (str): The unique identifier of the resource to list
                revisions for.

        Returns:
            Generator[str]: A generator yielding all revision IDs for the
                specified resource.

        Raises:
            ResourceIDNotFoundError: If the resource ID does not exist in the store.

        ---
        This method provides access to all revision identifiers for a specific
        resource, enabling complete history traversal and revision management.
        The revisions represent the complete change history of the resource.

        The iteration order is typically chronological (oldest to newest) but
        may vary depending on the implementation. For guaranteed ordering,
        consider using revision timestamps or sequence numbers.
        """

    @abstractmethod
    def exists(self, resource_id: str, revision_id: str) -> bool:
        """Check if a specific revision exists for a given resource.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision to check.

        Returns:
            bool: True if the specified revision exists, False otherwise.

        ---
        This method provides a fast existence check without retrieving the actual
        data. It's useful for validation and conditional logic before attempting
        to retrieve or operate on specific revisions.

        The method returns False if either the resource doesn't exist or the
        specific revision doesn't exist for that resource.
        """

    @abstractmethod
    def get(self, resource_id: str, revision_id: str) -> Resource[T]:
        """Retrieve a specific revision of a resource.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision to retrieve.

        Returns:
            Resource[T]: The complete resource object including both data and
                revision information for the specified revision.

        Raises:
            ResourceIDNotFoundError: If the resource ID does not exist.
            RevisionIDNotFoundError: If the revision ID does not exist for the resource.

        ---
        This method retrieves the complete resource data and metadata for a specific
        revision. The returned Resource object contains both the data as it existed
        at that revision and the RevisionInfo with metadata about that revision.

        This is the primary method for accessing historical data and enables
        point-in-time recovery and data comparison between revisions.
        """

    @abstractmethod
    def get_revision_info(self, resource_id: str, revision_id: str) -> RevisionInfo:
        """Retrieve revision metadata without the resource data.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision.

        Returns:
            RevisionInfo: The revision metadata including timestamps, user info,
                parent revision references, and other revision-specific information.

        Raises:
            ResourceIDNotFoundError: If the resource ID does not exist.
            RevisionIDNotFoundError: If the revision ID does not exist for the resource.

        ---
        This method provides access to revision metadata without loading the
        potentially large resource data. It's useful for revision browsing,
        audit trails, and operations that only need revision metadata.

        The RevisionInfo includes information such as:
        - Creation and update timestamps
        - User information (who created/updated)
        - Parent revision relationships
        - Revision status and schema version
        - Data integrity hashes
        """

    @abstractmethod
    def save(self, data: Resource[T]) -> None:
        """Store a complete resource revision.

        Arguments:
            data (Resource[T]): The complete resource object including both
                data content and revision information to be stored.

        ---
        This method stores a complete resource revision, including both the data
        content and all associated revision metadata. If a revision with the same
        resource_id and revision_id already exists, it will be replaced.

        The save operation should be atomic where possible, ensuring that both
        the data and revision information are stored consistently. The method
        should handle serialization of the data content according to the store's
        encoding strategy.

        Error handling should ensure that partial writes don't leave the store
        in an inconsistent state, and appropriate exceptions should be raised
        for storage failures or constraint violations.
        """

    @abstractmethod
    def encode(self, data: T) -> bytes:
        """Encode resource data into bytes for storage.

        Arguments:
            data (T): The resource data object to encode.

        Returns:
            bytes: The encoded representation of the data suitable for storage.

        ---
        This method handles the serialization of resource data into a byte format
        suitable for storage in the underlying storage system. The encoding method
        should be consistent and reversible, allowing the data to be accurately
        reconstructed when retrieved.

        The encoding strategy may vary depending on the store implementation:
        - JSON encoding for text-based storage
        - MessagePack for binary efficiency
        - Custom encoding for specialized data types

        The method should handle type-specific serialization requirements and
        maintain data integrity during the encoding process.
        """


class IStorage(ABC, Generic[T]):
    """Interface for unified storage management combining metadata and resource data.

    This interface provides a high-level abstraction that combines both metadata
    storage (IMetaStore) and resource data storage (IResourceStore) into a single
    unified interface. It serves as the primary storage abstraction for the
    ResourceManager and handles the coordination between metadata and data storage.

    The storage interface manages the complete lifecycle of resources including:
    - Resource and revision existence checking
    - Metadata and data storage coordination
    - Search and query operations across metadata
    - Bulk data export and import operations
    - Data encoding and serialization

    Type Parameters:
        T: The type of data stored in resources. This ensures type safety
           for resource data operations throughout the storage layer.

    This interface is typically implemented by storage systems that coordinate
    between separate metadata and resource stores, providing a unified view
    while optimizing for different access patterns and performance requirements.
    """

    @abstractmethod
    def exists(self, resource_id: str) -> bool:
        """Check if a resource exists in the storage system.

        Arguments:
            resource_id (str): The unique identifier of the resource to check.

        Returns:
            bool: True if the resource exists (has metadata), False otherwise.

        ---
        This method checks for resource existence at the metadata level. A resource
        is considered to exist if it has associated metadata, regardless of its
        deletion status or the number of revisions it has.

        This is a lightweight operation that only checks metadata presence and
        does not verify the existence of specific revisions or data integrity.
        """

    @abstractmethod
    def revision_exists(self, resource_id: str, revision_id: str) -> bool:
        """Check if a specific revision exists for a given resource.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision to check.

        Returns:
            bool: True if the specified revision exists, False otherwise.

        ---
        This method verifies the existence of a specific revision within the
        resource's history. It checks both that the resource exists and that
        the particular revision is available in the storage system.

        This operation may involve checking both metadata consistency and
        data availability depending on the storage implementation.
        """

    @abstractmethod
    def get_meta(self, resource_id: str) -> ResourceMeta:
        """Retrieve metadata for a specific resource.

        Arguments:
            resource_id (str): The unique identifier of the resource.

        Returns:
            ResourceMeta: The complete metadata object for the resource.

        Raises:
            ResourceIDNotFoundError: If the resource does not exist.

        ---
        This method retrieves the complete metadata for a resource, including
        current revision information, timestamps, user data, deletion status,
        and indexed data for search operations.

        The metadata provides essential information about the resource without
        requiring access to the potentially large resource data content.
        """

    @abstractmethod
    def save_meta(self, meta: ResourceMeta) -> None:
        """Store or update metadata for a resource.

        Arguments:
            meta (ResourceMeta): The metadata object to store.

        ---
        This method stores or updates the metadata for a resource. If metadata
        for the resource already exists, it will be replaced. The operation
        should be atomic and ensure consistency between the metadata and any
        associated indexes.

        The method handles persistence of all metadata fields including indexed
        data that may be used for search operations.
        """

    @abstractmethod
    def list_revisions(self, resource_id: str) -> list[str]:
        """List all revision IDs for a specific resource.

        Arguments:
            resource_id (str): The unique identifier of the resource.

        Returns:
            list[str]: A list of all revision IDs for the resource, typically
                ordered chronologically from oldest to newest.

        Raises:
            ResourceIDNotFoundError: If the resource does not exist.

        ---
        This method provides a complete list of all revisions available for a
        resource, enabling full history traversal and revision management
        operations. The ordering facilitates understanding the evolution of
        the resource over time.
        """

    @abstractmethod
    def get_resource_revision(self, resource_id: str, revision_id: str) -> Resource[T]:
        """Retrieve a specific revision of a resource with complete data.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision.

        Returns:
            Resource[T]: The complete resource object including both data
                and revision information.

        Raises:
            ResourceIDNotFoundError: If the resource does not exist.
            RevisionIDNotFoundError: If the revision does not exist.

        ---
        This method retrieves the complete resource data and metadata for a
        specific revision, providing access to the resource as it existed at
        that point in time. This enables point-in-time recovery and historical
        data analysis.
        """

    @abstractmethod
    def get_resource_revision_info(
        self,
        resource_id: str,
        revision_id: str,
    ) -> RevisionInfo:
        """Retrieve revision information without the resource data.

        Arguments:
            resource_id (str): The unique identifier of the resource.
            revision_id (str): The unique identifier of the revision.

        Returns:
            RevisionInfo: The revision metadata including creation info,
                parent relationships, and status information.

        Raises:
            ResourceIDNotFoundError: If the resource does not exist.
            RevisionIDNotFoundError: If the revision does not exist.

        ---
        This method provides access to revision metadata without loading the
        potentially large resource data. It's optimized for operations that
        only need revision information such as audit trails, version browsing,
        and revision relationship analysis.
        """

    @abstractmethod
    def save_resource_revision(self, resource: Resource[T]) -> None:
        """Store a complete resource revision including data and metadata.

        Arguments:
            resource (Resource[T]): The complete resource object to store.

        ---
        This method stores a complete resource revision, coordinating the storage
        of both the resource data and its revision information. The operation
        should ensure consistency between the data and metadata components.

        The method handles serialization of the resource data and proper indexing
        for search operations while maintaining referential integrity between
        revisions and their parent relationships.
        """

    @abstractmethod
    def search(self, query: ResourceMetaSearchQuery) -> list[ResourceMeta]:
        """Search for resources based on metadata and data criteria.

        Arguments:
            query (ResourceMetaSearchQuery): The search criteria including
                filters, sorting, and pagination parameters.

        Returns:
            list[ResourceMeta]: A list of metadata objects for resources
                that match the search criteria.

        ---
        This method provides comprehensive search capabilities across both
        metadata fields and indexed resource data. It supports complex
        filtering, sorting, and pagination to enable efficient resource
        discovery and management operations.

        The search operates on the metadata level but can filter based on
        indexed data content, providing powerful query capabilities without
        requiring full data loading for each resource.
        """

    @abstractmethod
    def encode_data(self, data: T) -> bytes:
        """Encode resource data for storage.

        Arguments:
            data (T): The resource data to encode.

        Returns:
            bytes: The encoded data suitable for storage.

        ---
        This method handles the serialization of resource data into a format
        suitable for storage. It coordinates with the underlying storage
        systems to ensure consistent encoding strategies and data integrity.

        The encoding method should be reversible and maintain data fidelity
        across storage and retrieval operations.
        """

    def list_resource_ids(self) -> list[str]:
        """Every resource id, materialised.

        Used by exports that then read each resource's files: holding a
        streaming cursor open across that I/O keeps a SQLite SHARED lock for
        the whole pass and blocks a live service's commits. Backends that
        can list ids cheaply should override this.
        """
        return [meta.resource_id for meta in self.dump_meta()]

    @abstractmethod
    def dump_meta(self) -> Generator[ResourceMeta]:
        """Export all resource metadata for backup or migration.

        Returns:
            Generator[ResourceMeta]: A generator yielding all metadata
                objects in the storage system.

        ---
        This method provides a way to export all resource metadata for backup,
        migration, or analysis purposes. It iterates through all resources
        regardless of their deletion status, providing complete metadata
        coverage.

        The generator approach allows for memory-efficient processing of large
        datasets without loading all metadata into memory simultaneously.
        """

    @abstractmethod
    def dump_resource(self) -> Generator[Resource[T]]:
        """Export all resource data including complete revision information.

        Returns:
            Generator[Resource[T]]: A generator yielding all resource objects
                with their complete data and revision information.

        ---
        This method provides comprehensive data export capabilities, including
        both resource data and revision metadata. It's designed for complete
        system backups and data migration scenarios where full data fidelity
        is required.

        The export includes all revisions of all resources, providing complete
        historical data preservation. The generator approach enables processing
        of large datasets efficiently.
        """


# Data Search Related Classes


class UnifiedSortKey(StrEnum):
    # Meta 欄位
    created_time = "created_time"
    updated_time = "updated_time"
    resource_id = "resource_id"

    # Data 欄位（用前綴區分）
    data_prefix = "data."  # 實際使用時會是 "data.name", "data.user.email" 等


class UnifiedSearchSort(Struct, kw_only=True):
    direction: ResourceMetaSortDirection = ResourceMetaSortDirection.ascending
    key: str  # 可以是 meta 欄位名或 "data.field_path"


class IndexEntry(Struct, kw_only=True):
    resource_id: str
    revision_id: str
    field_path: str
    field_value: Any
    field_type: str  # Store type name as string
