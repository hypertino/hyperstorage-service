create table content (
	document_uri text,				-- Unique document path
	item_id text,					-- collection item identifier, empty string for a document
	revision bigint static,			-- revision of document
	transaction_list list<timeuuid> static,	-- transactions in progress TODO: this leads to tombstone!!!!
	is_deleted boolean static,			-- flag that document is deleted (collection items removed from table)
	count bigint static,                         -- count of items in collection
	is_view boolean static,                      -- view, can't be updated directly
	body text,					-- body content
	created_at timestamp,
	modified_at timestamp,
	ttl int,                                     -- we use this because selecting ttl(body) returns null just before expiration
	primary key (document_uri, item_id)
);

create table transaction (
	dt_quantum bigint,				-- identifies quantum (slice) of time when transaction was created
	partition int,					-- partition calculated from document_uri (crc)
	document_uri text,
	item_id text,
	uuid timeuuid,					-- unique identifier of transaction, included into content.transaction_list
	revision bigint,				-- revision that corresponds to content.revision
	body text,					-- transaction body
	obsolete_index_items text,			-- identifies items become obsolete for each index by this transaction, JSON encoded
	completed_at timestamp,				-- null until transaction is complete
	primary key ((dt_quantum, partition), document_uri, uuid)
);

create table checkpoint (
	partition int primary key,			-- partition corresponds to transaction.partition
	last_quantum bigint				-- last quantum that is checked by Recovery Workers
);

create table index_def(
    document_uri text,					-- document (collection) uri
    index_id text,					-- index id
    status int,						-- 0 - new index (in progress), 1 - deleting (in progress), 2 - ready (indexed)
    sort_by text,					-- index sort expression, JSON encoded
    filter_by text,					-- filter expression
    table_name text,					-- table name with index content
    def_transaction_id timeuuid,			-- identifies ID of transaction creating index (see pending index)
    materialize boolean,                                -- copy data into the index to avoid access to main table when selecting
    primary key (document_uri, index_id)
);

create table view_def(
    key text,                                           -- '*' for all records
    document_uri text,                                  -- document_uri to lookup in index_def,
    template_uri text,                                  -- view uri template
    filter_by text,					-- filter expression
    primary key (key, document_uri)
);

create table pending_index(
    partition int,
    document_uri text,
    index_id text,
    last_item_id text,					-- last item that was indexed (this applies when we index collection first time)
    def_transaction_id timeuuid,			-- should match to index_def.def_transaction_id
    primary key (partition, document_uri, index_id, def_transaction_id)
);

create table index_content(
    document_uri text,
    index_id text,
    item_id text,
    revision bigint static,
    count bigint static,
    body text,
    created_at timestamp,
    modified_at timestamp,
    primary key ((document_uri, index_id), item_id)
);

create table index_content_ta0(
    document_uri text,
    index_id text,
    t0 text,
    item_id text,
    revision bigint static,
    count bigint static,
    body text,
    created_at timestamp,
    modified_at timestamp,
    primary key ((document_uri, index_id), t0, item_id)
);

create table index_content_da0(
    document_uri text,
    index_id text,
    d0 decimal,
    item_id text,
    revision bigint static,
    count bigint static,
    body text,
    created_at timestamp,
    modified_at timestamp,
    primary key ((document_uri, index_id), d0, item_id)
);

create table index_content_td0(
    document_uri text,
    index_id text,
    t0 text,
    item_id text,
    revision bigint static,
    count bigint static,
    body text,
    created_at timestamp,
    modified_at timestamp,
    primary key ((document_uri, index_id), t0, item_id)
) with clustering order by (t0 desc);

create table index_content_dd0(
    document_uri text,
    index_id text,
    d0 decimal,
    item_id text,
    revision bigint static,
    count bigint static,
    body text,
    created_at timestamp,
    modified_at timestamp,
    primary key ((document_uri, index_id), d0, item_id)
) with clustering order by (d0 desc);

